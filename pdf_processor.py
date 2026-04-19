import fitz  # PyMuPDF
import sys
import os
import string
import itertools
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

def log_progress(msg):
    print(f"PROGRESS|{msg}", flush=True)

# 🟢 Prefix Generator
def get_prefixes(name):
    # Remove everything except letters and take first 4 (Uppercase)
    clean = "".join(filter(str.isalpha, name)).upper()
    if len(clean) >= 4:
        return [clean[:4]]
    else:
        # If name is shorter than 4 characters, pad with spaces
        return [clean.ljust(4, ' ')[:4]]

def check_range(pdf_path, prefixes, year_range):
    """Worker function to check a list of prefixes against a year range."""
    # Each worker opens its own copy of the PDF
    try:
        doc = fitz.open(pdf_path)
        for prefix in prefixes:
            for year in year_range:
                t_pass = f"{prefix}{year}"
                if doc.authenticate(t_pass):
                    doc.close()
                    return t_pass
        doc.close()
    except:
        pass
    return None

def main():
    if len(sys.argv) < 6:
        print("ERROR|Missing arguments")
        sys.exit(1)

    pdf_path = sys.argv[1]
    name_hint = sys.argv[2]
    output_dir = sys.argv[3]
    req_id = sys.argv[4]
    is_premium = sys.argv[5].lower() == 'true'

    unlocked_pdf = os.path.join(output_dir, f"Unlocked_{req_id}.pdf")
    front_img = os.path.join(output_dir, f"Front_{req_id}.jpg")
    back_img = os.path.join(output_dir, f"Back_{req_id}.jpg")

    try:
        doc = fitz.open(pdf_path)
        if not doc:
            print("ERROR|Failed to open PDF")
            sys.exit(1)

        final_password = ""
        if doc.is_encrypted:
            auth_success = False
            
            # --- LEVEL 1: Smart Guess ---
            prefixes_smart = get_prefixes(name_hint)
            log_progress(f"Starting Smart Guess scan...")
            for p in prefixes_smart:
                for y in range(1940, 2027):
                    t_pass = f"{p}{y}"
                    if doc.authenticate(t_pass):
                        final_password = t_pass
                        auth_success = True
                        break
                if auth_success: break
            
            # --- LEVEL 2: High-Speed Multi-Core Brute Force ---
            if not auth_success:
                if not is_premium:
                    print("ERROR|Brute-Force is a Premium Feature.")
                    sys.exit(1)

                log_progress("Initializing High-Speed Brute Force (Multi-Core)...")
                
                # Setup search space
                chars = string.ascii_uppercase
                # Prioritize common starting letters locally for Aadhaar
                priority = "ARSPMKVJ"
                ordered_chars = priority + "".join([c for c in chars if c not in priority])
                
                # Full 4-char space
                all_prefixes = [''.join(p) for p in itertools.product(ordered_chars, repeat=4)]
                years = list(range(1940, 2027))
                
                # Chunking
                cpu_count = os.cpu_count() or 4
                chunk_size = len(all_prefixes) // (cpu_count * 2)
                if chunk_size == 0: chunk_size = 1
                
                chunks = [all_prefixes[i:i + chunk_size] for i in range(0, len(all_prefixes), chunk_size)]
                
                log_progress(f"Spawning {cpu_count} workers for 40M combinations...")
                
                with ProcessPoolExecutor(max_workers=cpu_count) as executor:
                    futures = {executor.submit(check_range, pdf_path, chunk, years): chunk for chunk in chunks}
                    
                    completed = 0
                    for future in as_completed(futures):
                        result = future.result()
                        completed += 1
                        if result:
                            final_password = result
                            auth_success = True
                            # Cancel other futures
                            for f in futures: f.cancel()
                            break
                        
                        if completed % 10 == 0:
                            log_progress(f"Deep Search: {completed}/{len(chunks)} blocks verified...")

            if not auth_success:
                print("ERROR|Password not found.")
                sys.exit(1)

            # Re-verify and save
            doc.authenticate(final_password)
        
        doc.save(unlocked_pdf)
        
        # Extract Aadhaar Number
        aadhaar_number = "Not Found"
        page = doc[0]
        text = page.get_text("text")
        for line in text.split('\n'):
            line = line.strip()
            if len(line) == 14 and " " in line:
                parts = line.split(" ")
                if len(parts) == 3 and all(p.isdigit() for p in parts):
                    aadhaar_number = line
                    break
        
        # Render Images
        mat = fitz.Matrix(5, 5)
        w, h = page.rect.width, page.rect.height
        front_rect = fitz.Rect(w * 0.05, h * 0.64, w * 0.49, h * 0.96)
        back_rect = fitz.Rect(w * 0.51, h * 0.64, w * 0.95, h * 0.96)
        
        page.get_pixmap(matrix=mat, clip=front_rect).save(front_img)
        page.get_pixmap(matrix=mat, clip=back_rect).save(back_img)
        
        doc.close()
        print(f"SUCCESS|{aadhaar_number}|{unlocked_pdf}|{front_img}|{back_img}|{final_password}")

    except Exception as e:
        print(f"ERROR|{str(e)}")
        sys.exit(2)

if __name__ == "__main__":
    main()
