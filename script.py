
def check_page_contents(tablePath: str):
    with open(tablePath, 'rb') as file:
        binary_data = file.read()
        
    print(f"Total Bytes: {len(binary_data)}")
    
    # Filter out null bytes and unprintable characters to see if your text is in there
    printable_chars = [chr(b) for b in binary_data if 32 <= b <= 126]
    print("Found text:", "".join(printable_chars))
    
def hex_dump_page(tablePath: str, bytes_to_read=128):
    with open(tablePath, 'rb') as file:
        binary_data = file.read(bytes_to_read)
        
    print(f"--- HEX DUMP (First {bytes_to_read} bytes) ---")
    for i in range(0, len(binary_data), 16):
        chunk = binary_data[i:i+16]
        
        # Format as Hex (e.g., 0A 00 00 00)
        hex_str = " ".join([f"{b:02X}" for b in chunk])
        
        # Format as ASCII (replacing non-printable with '.')
        ascii_str = "".join([chr(b) if 32 <= b <= 126 else "." for b in chunk])
        
        # Print with offset
        print(f"{i:04X} | {hex_str:<47} | {ascii_str}")


def main():
    tablePath = "/home/nines/Desktop/gon/TestDB/s7/tb.tbl"
    print("hitting check page contents")
    check_page_contents(tablePath)
    print("hex dump")
    hex_dump_page(tablePath)

main()
if __name__ == "main":
    main()

