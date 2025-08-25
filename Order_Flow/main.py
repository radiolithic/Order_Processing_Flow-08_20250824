#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py - Main entry point for the Order Flow process

This script orchestrates the entire order synchronization and reporting process:
1. Updates Shopify orders in the database
2. Refreshes Odoo orders in the database
3. Generates an Excel report comparing the orders
4. Uploads the report to Odoo via API

The script uses subprocess to run each component and handles errors appropriately.
"""

import subprocess
import sys
import os
import logging
from datetime import datetime
import argparse

# --- Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Logging Configuration ---
LOG_FILE = os.path.join(OUTPUT_DIR, "order_flow.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_script(script_name, days=None, all_flag=False):
    """
    Runs a Python script as a subprocess, passing arguments.
    
    Args:
        script_name: Name of the script to run
        days: Number of days of orders to fetch
        all_flag: Boolean to fetch all orders
        
    Returns:
        True if successful, False otherwise
    """
    script_path = os.path.join(SCRIPT_DIR, script_name)
    
    # Build command with arguments
    command = [sys.executable, script_path]
    if all_flag:
        command.append('--all')
        logger.info(f"Running {script_name} with --all flag...")
    elif days is not None:
        command.extend(['--days', str(days)])
        logger.info(f"Running {script_name} for the last {days} days...")
    else:
        logger.info(f"Running {script_name}...")

    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        
        # Log the output
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"[{script_name}] {line}")
        
        logger.info(f"{script_name} completed successfully")
        return True
    
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}: {e}")
        
        # Log the error output
        if e.stdout:
            for line in e.stdout.splitlines():
                logger.info(f"[{script_name}] {line}")
        
        if e.stderr:
            for line in e.stderr.splitlines():
                logger.error(f"[{script_name}] {line}")
        
        return False
    
    except Exception as e:
        logger.error(f"Unexpected error running {script_name}: {e}")
        return False

def main():
    """
    Main function to run the entire order synchronization and reporting process.
    """
    start_time = datetime.now()
    
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Run the full Order Flow process.")
    parser.add_argument('--days', type=int, default=90,
                        help='Number of days of orders to fetch (default: 90)')
    parser.add_argument('--all', action='store_true',
                        help='Fetch all orders, overriding --days')
    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("  WOODLANDERS ORDER SYNC AND REPORTING SYSTEM")
    logger.info(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    try:
        # Step 1: Update Shopify orders
        if not run_script("update_shopify_orders.py", days=args.days, all_flag=args.all):
            logger.error("Failed to update Shopify orders. Aborting.")
            sys.exit(1)
        
        # Step 2: Refresh Odoo orders
        if not run_script("refresh_odoo_orders.py", days=args.days, all_flag=args.all):
            logger.error("Failed to refresh Odoo orders. Aborting.")
            sys.exit(1)
        
        # Step 3: Generate Excel report and upload to Odoo
        if not run_script("create_excel_report.py"): # This script does not need date arguments
            logger.error("Failed to create Excel report. Aborting.")
            sys.exit(1)
        
        # Optional: Compare orders for detailed analysis
        run_script("compare_orders.py")
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("  ORDER SYNC AND REPORTING COMPLETED SUCCESSFULLY")
        logger.info(f"  Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"  Duration: {duration}")
        logger.info("  The order flow report has been generated and uploaded to Odoo")
        logger.info("=" * 80)
        
        return 0
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)