class EpdProc:
    def decompress_df(self, df, num_sectors, table):
        """Decompresses a DataFrame of compressed packets

        How EPD Compression Works:
        * Each sector has 16 bins associated with it (0 - 15)
        * One period is 16 sectors
        * Each packet corresponds to one period
        * Think of packets in groups of 10: 1 Header packet, which holds the
        actual values; and 9 Non-header packets, which don't store the actual
        values but the change that needs to be applied to the previous packet
        in order to get the value (ex. if the actual value is 5, and the
        previous value is 3, then 2 will be stored)
        * For ALL packets, these 'delta' values aren't actually stored. The
        sign is either + or -, which is stored for NON-HEADER packets. There
        is a table of 255 values. Instead of storing the actual values or
        deltas (depending on if it's a header or non-header), ALL PACKETS hold
        the indices of the closest values. These indexes are further reduced
        in size through Huffman Compression. Find the table and the Huffman
        Compression decoder in parse_log.py

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame of EPD data to be decompressed
        num_sectors : int
            The number of sectors to be used in calculations
        table : dict
            The table to be referenced for Huffman compression

        Returns
        -------
        pd.DataFrame
            A DataFrame of decompressed data
        """
        # l0_df: holds finished periods
        # measured values: For one period, hold value from Huffman table and later is put into period_df
        data = df["data"]
        l0_df = pd.DataFrame(columns=["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data"])
        measured_values = [None] * BIN_COUNT * num_sectors

        # Set the index to initially point to the first header
        packet_num = self.find_first_header(data)
        if packet_num == data.shape[0]:
            return l0_df

        lossy_idx = self.find_lossy_idx(data.iloc[packet_num])
        marker = 0xAA + lossy_idx  # Determines if a packet is a header
        consecutive_nonheader_packets = 0
        needs_header = True  # Determine if we need to search for a new header, ex. bc of problem with current packet

        while packet_num < data.shape[0]:
            cur_data = data.iloc[packet_num]
            if cur_data[10] == marker:  # Current packet is a header (reference frame)
                measured_values = self.get_measured_values_if_valid(num_sectors, packet_num, cur_data)
                needs_header = len(measured_values) == 0

            else:  # We get a non-header/non-reference frame/continuation frame
                if needs_header:
                    raise ValueError(f"Needed header at {packet_num}, but this shouldn't happen!")

                needs_header = self.update_measured_values_if_valid(num_sectors, measured_values, packet_num, cur_data, table)
                consecutive_nonheader_packets += 1

            # Append the period (found from either the header or non-header packet) to l0_df
            if not needs_header:
                period_df = self.get_period_df(
                    measured_values, EPD_LOSSY_VALS[lossy_idx], cur_data, df.iloc[packet_num]
                )
                l0_df = l0_df.append(period_df, sort=True)

            # Get the next needed packet (perhaps a header is needed if something was wrong with a previous packet)
            packet_num += 1
            if not data.iloc[packet_num] or needs_header or consecutive_nonheader_packets >= 9:
                packet_num += self.find_first_header(data.iloc[packet_num:])
                consecutive_nonheader_packets = 0

        return l0_df.reset_index(drop=True)

    def get_measured_values_if_valid(self, num_sectors, packet_num, cur_data):
        """Returns a list to replace measured_values, will be empty if bad cur_data"""
        values = cur_data[11:]
        if len(values) != BIN_COUNT * num_sectors:
            self.logger.warning(f"⚠️ Header at packet number {packet_num} didn't have all reference bins")
            return []
        return [val for val in values]

    def update_measured_values_if_valid(self, num_sectors, measured_values, packet_num, cur_data, table):
        """TODO: Modifies measured_values in place, but maybe it's better to just return it"""
        bitstring = byte_tools.bin_string(cur_data[10:])
        needs_header = False
        for i in range(BIN_COUNT * num_sectors):  # Updating each of the 256 values in the period
            try:
                sign, bitstring = self.get_sign(bitstring)
            except ValueError as e:
                self.logger.warning(f"⚠️ Bad Sign: {e}, ind: {i}, packet {packet_num}")
                needs_header = True

            try:
                delta1, bitstring = byte_tools.get_huffman(bitstring, table)
                delta2, bitstring = byte_tools.get_huffman(bitstring, table)
            except IndexError as e:
                self.logger.warning(f"⚠️ Not enough bytes in continuation packet: {e}, packet {packet_num}")
                needs_header = True

            measured_values[i] += sign * ((delta1 << 4) + delta2)

            if not 0 <= measured_values[i] <= 255:
                self.logger.warning(f"⚠️ measured_values went out of range [0, 255]: {measured_values[i]}")
                needs_header = True

            if needs_header:
                break

        return needs_header

    @staticmethod
    def find_lossy_idx(data_packet_in_bytes):
        return data_packet_in_bytes[10] - 0xAA

    @staticmethod
    def find_first_header(data):
        packet_num = 0
        while packet_num < data.shape[0]:
            if data.iloc[packet_num] is None or data.iloc[packet_num][10] & 0xA0 != 0xA0:
                packet_num += 1
            else:
                return packet_num
        return packet_num  # Went out of bounds, no header found

    @staticmethod
    def get_sign(bitstring):
        if bitstring[:2] == "00":
            return 1, bitstring[2:]
        if bitstring[:2] == "01":
            return -1, bitstring[2:]
        raise ValueError(f"Got {bitstring[:2]} instead of '00' or '01'")

    def get_period_df(self, measured_values, lossy_vals, cur_data, row):
        """
        Using the indices found, as well as the table of 255 values, find the values
        for a period, and return it as a DataFrame. Basically, this is used to create new
        'uncompressed' packets that get added to the level 0 DataFrame

        lossy_vals is the table of 255 potential values
        loss_val_idx is one of the indices that were found from the compressed packets
        lossy_val is the value found using lossy_vals and lossy_val_idx
        """
        self.logger.debug("Formatting period to level 0 df")
        spin_period_bytes, collection_time_bytes = cur_data[8:10], cur_data[:8]

        # Add the time
        bytes_data = spin_period_bytes + collection_time_bytes
        num_sectors = len(measured_values) / 16
        if num_sectors not in VALID_NUM_SECTORS:
            raise ValueError(f"Bad Number of Sectors: {num_sectors}")

        num_bins = 0
        sector_num = 0x0F
        bytes_data += bytes([sector_num])

        for loss_val_idx in measured_values:

            # If the sector is complete, then increment the sector_num and prepare for a new sector
            if num_bins == 16:
                sector_num += 0x10 if num_sectors == 16 else 0x40
                bytes_data += bytes([sector_num])
                num_bins = 0

            lossy_val = lossy_vals[loss_val_idx]
            bytes_data += byte_tools.get_two_unsigned_bytes(lossy_val & 0xFFFF)  # least significant word first
            bytes_data += byte_tools.get_two_unsigned_bytes(lossy_val >> 16)  # most significant word next
            num_bins += 1

        data = bytes_data.hex()

        period_df = pd.DataFrame(
            data={
                    "idpu_time": byte_tools.raw_idpu_bytes_to_datetime(collection_time_bytes),
                    "data": data,
                    "mission_id": row["mission_id"],
                    "idpu_type": row["idpu_type"],
                    "numerator": row["numerator"],
                    "denominator": row["denominator"]

            }, index=[0]
        )
        return period_df
