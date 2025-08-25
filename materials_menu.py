#!/usr/bin/env python3
"""
Materials Management Menu System

This script provides a simple menu interface for running the various 
Materials management tasks for stock and orders across Shopify and Odoo.

Simply select an option from the menu to execute the corresponding task.
"""

import os
import sys
import subprocess
import time
from datetime import datetime

# ANSI color codes for terminal output (Windows-compatible empty strings)
class Colors:
    HEADER = ''
    BLUE = ''
    GREEN = ''
    YELLOW = ''
    RED = ''
    ENDC = ''
    BOLD = ''
    UNDERLINE = ''

# Get the directory containing this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def clear_screen():
    """Clear the terminal screen."""
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def print_header():
    """Print the header for the menu."""
    clear_screen()
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'MATERIALS MANAGEMENT SYSTEM':^80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print()

def run_batch_file(batch_file, description):
    """Run a batch file and wait for it to complete."""
    print(f"\n{Colors.BLUE}Running: {description}...{Colors.ENDC}")
    print(f"{Colors.YELLOW}{'=' * 80}{Colors.ENDC}")
    
    # Full path to the batch file
    batch_path = os.path.join(SCRIPT_DIR, batch_file)
    
    try:
        # Use subprocess.call so the output appears in the terminal
        # On Windows, shell=True is required for batch files
        subprocess.call(batch_path, shell=True)
        print(f"\n{Colors.GREEN}Completed: {description}{Colors.ENDC}")
    except Exception as e:
        print(f"\n{Colors.RED}Error executing {batch_file}: {e}{Colors.ENDC}")
    
    # Pause to let user see the results
    input(f"\n{Colors.YELLOW}Press Enter to return to the menu...{Colors.ENDC}")

def main_menu():
    """Display the main menu and handle user input."""
    while True:
        print_header()
        
        # Display menu options
        print(f"{Colors.BOLD}[1] {Colors.BLUE}Import Shopify Data{Colors.ENDC}")
        print(f"    Process Shopify exports and update the database")
        print()
        
        print(f"{Colors.BOLD}[2] {Colors.BLUE}Run Order Flow{Colors.ENDC}")
        print(f"    Synchronize and report on orders between Shopify and Odoo")
        print()
        
        print(f"{Colors.BOLD}[3] {Colors.BLUE}Stock Cross Reference{Colors.ENDC}")
        print(f"    Generate inventory reconciliation between Shopify and Odoo")
        print()
        
        print(f"{Colors.BOLD}[4] {Colors.BLUE}Generate Pull Sheet{Colors.ENDC}")
        print(f"    Create a pull sheet for fulfilling orders")
        print()
        
        print(f"{Colors.BOLD}[0] {Colors.RED}Exit{Colors.ENDC}")
        print()
        
        # Get user selection
        choice = input(f"{Colors.GREEN}Enter your choice (0-4): {Colors.ENDC}")
        
        if choice == '0':
            print(f"\n{Colors.YELLOW}Exiting Materials Management System. Goodbye!{Colors.ENDC}")
            break
        elif choice == '1':
            run_batch_file("RUN_IMPORT.bat", "Importing Shopify Data")
        elif choice == '2':
            run_batch_file("RUN_ORDER_FLOW.bat", "Order Flow Process")
        elif choice == '3':
            run_batch_file("RUN_STOCK_XREF.bat", "Stock Cross Reference")
        elif choice == '4':
            run_batch_file("RUN_PULL.bat", "Generating Pull Sheet")
        else:
            print(f"\n{Colors.RED}Invalid choice. Please try again.{Colors.ENDC}")
            time.sleep(1.5)

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Operation cancelled by user. Exiting...{Colors.ENDC}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Colors.RED}An unexpected error occurred: {e}{Colors.ENDC}")
        input("Press Enter to exit...")
        sys.exit(1)