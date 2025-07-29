#!/usr/bin/env python3
"""
Crypto Arbitrage Bot Module
--------------------------
Refactored version incorporating dynamic position sizing based on portfolio balance,
tier-specific allocation strategies, external token specifications from trading_pairs.py,
and aligned strategy definitions based on the Telegram Bot workflow.
"""

import requests
import time
import json
import base64
import hashlib
import hmac
import urllib.parse
import websocket
import threading
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import pytz
from typing import Dict, Optional, Tuple, List, Union, Any, Set
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
import queue
import os
import configparser
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.exchange import Exchange
from hyperliquid.utils.types import Meta
from eth_account import Account
import string

# Assume trading_pairs.py exists in the same directory and defines AVAILABLE_PAIRS
try:
    # Use the actual name from the file
    from trading_pairs import AVAILABLE_PAIRS as TOKEN_SPECIFICATIONS
except ImportError:
    print("ERROR: trading_pairs.py not found or AVAILABLE_PAIRS not defined.")
    # Provide a minimal fallback if needed, though the bot might not function
    TOKEN_SPECIFICATIONS = {}


class ArbBotBase:
    """Base class for arbitrage bots with common functionality and dynamic sizing"""

    # --- Constants ---
    HL_MIN_ORDER_USD = 5.0
    EXCHANGE_SAFETY_BUFFER_USD = 1.0 # Keep $1 buffer on each exchange

    def __init__(self, config_path=None, user_id=None, selected_tokens=None, tier=1):
        """Initialize the arbitrage bot"""
        self.config_path = config_path
        self.user_id = user_id or "default"
        self.tier = tier # Tier (1, 2, or 3) determines allocation logic
        self.selected_tokens = selected_tokens if selected_tokens is not None else [] # Ensure it's a list
        self.est_tz = pytz.timezone('US/Eastern')

        # Setup logging first
        self.setup_logging()
        self.logger.info(f"--- Initializing ArbBot for User: {self.user_id}, Tier: {self.tier} ---")

        # Load external token specifications
        self.token_specifications = TOKEN_SPECIFICATIONS
        if not self.token_specifications:
             self.logger.error("TOKEN_SPECIFICATIONS from trading_pairs.py is empty or failed to load!")

        # Initialize API clients, URLs, credentials (will be loaded)
        self.hl_exchange: Optional[Exchange] = None
        self.hl_info: Optional[Info] = None
        self.hl_wallet: Optional[str] = None # Wallet address loaded from config
        self.hl_ws_url = "wss://api.hyperliquid.xyz/ws"
        self.hl_api_url = constants.MAINNET_API_URL # Use constant from SDK
        self.kraken_api_url = "https://api.kraken.com"
        self.kraken_key: Optional[str] = None
        self.kraken_secret: Optional[str] = None
        self.hl_key: Optional[str] = None # API key, maybe unused by SDK if private key used
        self.hl_secret: Optional[str] = None # Private key for signing

        # Initialize tracking variables
        self.active_positions = set() # Assets currently holding a position
        self.entry_candidates = set() # Assets meeting entry criteria, awaiting execution
        self.last_funding_rates: Dict[str, float] = {} # Stores {asset: current_rate_in_%} from WS/API
        # self.last_percentiles = {}   # Kept for potential future use or debugging
        self.historical_percentiles: Dict[str, Dict[str, float]] = {} # Stores {asset: {'60': value, '50': value}}
        self.kraken_call_times = deque(maxlen=50) # For rate limiting

        # Rate limiting
        self.kraken_api_last_call = 0.0
        self.kraken_api_min_interval = 0.35 # Current Kraken script uses 0.35
        self.kraken_order_check_interval = 3.0
        self.kraken_rate_limit_window = 60
        # Using intermediate tier limit from original script
        self.kraken_max_calls_per_window = 46

        # WebSocket
        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.ws_running = False
        self.ws_message_queue = queue.Queue()
        self.lock = threading.Lock()
        self.ws_reconnect_count = 0
        self.max_ws_reconnects = 10 # From original script

        # --- Aligned Strategy Definitions (Based on Telegram Bot Options) ---
        # Entry strategies map name to percentile value
        self.entry_percentiles = {
            'default': 60, # Default maps to 60th
            '50': 50,
            '75': 75,
            '85': 85,
            '95': 95,
            # 'abraxas' entry uses default 60th percentile
            'abraxas': 60
        }
        # Exit strategies map name to percentile value OR None if using time/predicted rate
        self.exit_percentiles = {
            'default': None,      # Uses time/predicted rate logic
            '50': 50,
            '35': 35,
            '20': 20,
            '10': 10,
            'exit_abraxas': None  # Abraxas Optimized Exit uses time/predicted rate logic
        }
        # These attributes will be set by load_custom_settings based on user's choice
        self.entry_strategy = "default" # Default if loading fails
        self.exit_strategy = "default"  # Default if loading fails

        # Safety checks
        self.last_margin_check_time = 0
        self.margin_check_interval = 60 # Current script uses 60
        self.min_margin_ratio = 0.15 # Current script uses 0.15
        self.max_price_deviation = 0.05 # Current script uses 0.05
        self.flash_crash_cooldowns: Dict[str, float] = {} # Track cooldown per asset
        
        # Trade management
        self.running = True
        # self.assets replaces self.positions and original self.assets structure
        # Structure: { 'BTC': { 'spec': {...}, 'in_position': False, 'position_size_hl_usd': None, ... } }
        self.assets: Dict[str, Dict[str, Any]] = {}
        self.supported_assets: List[str] = [] # List of assets configured for this bot instance
        self.order_history: Dict[str, List[Dict]] = {} # Store order attempts/results per asset
        
        # Error handling
        self.consecutive_errors = 0
        self.error_cooldown = 5 # Current script uses 5
        self.extended_cooldown = 30 # Current script uses 30
        self.max_consecutive_errors = 5 # Current script uses 5

        # Common configuration (Adopted from original script)
        self.exit_time_threshold = 15  # Minutes before hour end for 'default'/'exit_abraxas' check
        self.order_timeout = 60
        self.max_retries = 3
        # self.rate_check_interval = 5 # Maybe less critical if using WS primarily
        self.slippage_buffer = 0.001 # For limit orders if used

        # Load configuration and initialize - MUST call initialize() after object creation
        # self.initialize() # Call this externally after creating the bot object

    # --- Initialization Sequence ---
    
    def initialize(self):
        """Initialize the bot after configuration. Call this after creating the bot object."""
        self.logger.info("--- Starting Initialization ---")
        try:
            self.load_credentials() # Needs API keys and HL wallet address
            self.load_custom_settings() # Loads entry/exit strategy names chosen by user
            self.setup_apis() # Connects to exchanges, performs initial checks
            self.filter_assets_by_selected_tokens() # Sets up self.assets based on user selection & TOKEN_SPECIFICATIONS

            # Calculate historical percentiles for the assets the bot will actually trade
            if self.assets:
                self.logger.info(f"Calculating historical percentiles for: {list(self.assets.keys())}")
                all_percentiles_calculated = True
                assets_to_remove = []
                for asset in list(self.assets.keys()): # Use list() for safe iteration while modifying
                    # Determine which percentiles are needed based on loaded strategies
                    needed_percentiles = set()
                    entry_perc = self.entry_percentiles.get(self.entry_strategy)
                    if entry_perc is not None: needed_percentiles.add(entry_perc)
                    exit_perc = self.exit_percentiles.get(self.exit_strategy)
                    if exit_perc is not None: needed_percentiles.add(exit_perc)

                    # Calculate only the needed percentiles
                    percentiles = self.calculate_historical_percentile(asset, needed_percentiles)

                    if percentiles:
                        self.historical_percentiles[asset] = percentiles
                        # Log the specific percentiles calculated
                        log_str = f"Hist. percentiles for {asset}: "
                        if entry_perc is not None: log_str += f"Entry ({self.entry_strategy}={entry_perc}th)={percentiles.get(str(entry_perc), 'N/A'):.4f}% "
                        if exit_perc is not None: log_str += f"Exit ({self.exit_strategy}={exit_perc}th)={percentiles.get(str(exit_perc), 'N/A'):.4f}%"
                        self.logger.info(log_str)
                    else:
                        self.logger.warning(f"Failed percentile calc for {asset}. It will NOT be traded.")
                        all_percentiles_calculated = False
                        assets_to_remove.append(asset)

                # Remove assets for which percentile calculation failed
                for asset in assets_to_remove:
                    del self.assets[asset]

                self.supported_assets = list(self.assets.keys()) # Update supported assets list
                if not self.supported_assets: self.logger.error("No assets remaining after percentile check!")
                else: self.logger.info(f"Final tradeable assets: {self.supported_assets}")
                if not all_percentiles_calculated: self.logger.warning("Percentile calc failed for some selected assets.")
            else: self.logger.warning("No assets available after filtering.")

            # Start background threads only if there are assets to trade
            if self.supported_assets:
                self.start_message_processor() # For handling WS messages
                self.connect_websocket() # Start WS connection attempt
            else:
                self.logger.error("No assets configured to trade. Bot will not start WebSocket or trade.")
                self.running = False # Stop the bot if no assets

            self.logger.info("--- Initialization Complete ---")
        except Exception as e:
            self.logger.exception(f"FATAL ERROR during initialization: {e}")
            self.running = False # Prevent running if critical init step fails


    # --- Load/Setup Methods ---

    def setup_logging(self):
        """Setup logging configuration with log rotation (Unchanged from original)"""
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
        log_filename = f'arb_bot_{self.user_id}.log'
        file_handler = RotatingFileHandler(log_filename, maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger = logging.getLogger(f"arb_bot_{self.user_id}")
        self.logger.setLevel(logging.DEBUG) # Changed from INFO to DEBUG for more verbose logs
        if self.logger.hasHandlers(): self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.info(f"Logging setup. Log file: {log_filename}")
    
    def load_credentials(self):
        """Load API credentials and HL Wallet Address from configuration"""
        try:
            if self.config_path and os.path.exists(self.config_path):
                config = configparser.ConfigParser()
                config.read(self.config_path)
                # Kraken
                if 'kraken' in config and 'api_key' in config['kraken'] and 'api_secret' in config['kraken']:
                    self.kraken_key = config['kraken']['api_key']
                    self.kraken_secret = config['kraken']['api_secret']
                else: self.logger.warning("Kraken credentials missing in config.")
                # Hyperliquid - Secret (private key) and Wallet Address are mandatory for SDK
                if 'hyperliquid' in config and 'private_key' in config['hyperliquid'] and 'wallet_address' in config['hyperliquid']:
                    self.hl_secret = config['hyperliquid']['private_key']
                    self.hl_wallet = config['hyperliquid']['wallet_address']
                    self.hl_key = config['hyperliquid'].get('api_key') # Optional API key
                else: self.logger.warning("Hyperliquid private_key or wallet_address missing in config.")
                    
                if self.kraken_key and self.hl_secret and self.hl_wallet:
                    self.logger.info("Loaded credentials and wallet from config file")
                else:
                    raise ValueError("Missing required credentials (Kraken key/secret or HL private key/wallet) in config")
                
                # Fallback to module import (Keep or remove based on preference)
                self.logger.warning("Config file not found or specified, trying module import (legacy).")
                try:
                    from kraken_config import KRAKEN_API_KEY, KRAKEN_API_SECRET
                    self.kraken_key, self.kraken_secret = KRAKEN_API_KEY, KRAKEN_API_SECRET
                    # HL config module MUST also provide wallet address now
                    from hl_config import HL_API_KEY, HL_API_SECRET, HL_WALLET_ADDRESS
                    self.hl_key, self.hl_secret, self.hl_wallet = HL_API_KEY, HL_API_SECRET, HL_WALLET_ADDRESS
                    self.logger.info("Loaded credentials and wallet from config modules (legacy)")
                except ImportError as e:
                    self.logger.error(f"Legacy config module import failed: {e}")
                    raise Exception("No valid credentials found. Provide config file or ensure modules exist.") from e

        except Exception as e:
            self.logger.error(f"Error loading credentials: {e}")
            # Re-raise as initialization must fail if creds are missing
            raise Exception("Failed to load necessary API credentials/wallet address.") from e
    
    def load_custom_settings(self):
        """Load custom strategy settings chosen by the user (e.g., from config file)."""
        # Simplified to load only from config file for now. DB logic can be added.
        try:
            loaded_entry = None
            loaded_exit = None
            source = "defaults"
            if self.config_path and os.path.exists(self.config_path):
                config = configparser.ConfigParser()
                config.read(self.config_path)
                if 'Strategies' in config:
                    loaded_entry = config['Strategies'].get('entry_strategy')
                    loaded_exit = config['Strategies'].get('exit_strategy')
                    source = f"Config file ({self.config_path})"

            # Apply loaded or default values
            self.entry_strategy = loaded_entry if loaded_entry else 'default'
            self.exit_strategy = loaded_exit if loaded_exit else 'default'
            self.logger.info(f"Attempting to load strategies from {source}: Entry='{self.entry_strategy}', Exit='{self.exit_strategy}'")

            # --- Validate against ALIGNED strategy names ---
            # Entry Strategy Validation
            if self.entry_strategy not in self.entry_percentiles:
                 self.logger.warning(f"Configured entry strategy '{self.entry_strategy}' is not valid. Valid options: {list(self.entry_percentiles.keys())}. Using 'default'.")
                 self.entry_strategy = 'default'

            # Exit Strategy Validation
            # Valid exit strategies are those with percentiles OR the special 'default'/'exit_abraxas'
            valid_exit_keys = list(self.exit_percentiles.keys())
            if self.exit_strategy not in valid_exit_keys:
                 self.logger.warning(f"Configured exit strategy '{self.exit_strategy}' is not valid. Valid options: {valid_exit_keys}. Using 'default'.")
                 self.exit_strategy = 'default'

            self.logger.info(f"Final effective strategies: Entry='{self.entry_strategy}', Exit='{self.exit_strategy}'")
                    
        except Exception as e:
            self.logger.error(f"Error loading custom strategy settings: {str(e)}. Using defaults.")
            self.entry_strategy = 'default'
            self.exit_strategy = 'default'
    
    def setup_apis(self):
        """Initialize API connections and perform initial checks."""
        try:
            # --- Kraken Setup & Test ---
            if not self.kraken_key or not self.kraken_secret:
                raise ValueError("Kraken credentials not loaded.")
            self.logger.info("Testing Kraken API connection...")
            # Use existing kraken_request for the test
            kraken_balance_result = self.kraken_request('/0/private/Balance', {"nonce": str(int(1000*time.time()))})
            if not kraken_balance_result or ('error' in kraken_balance_result and kraken_balance_result['error']):
                raise Exception(f"Kraken API test failed: {kraken_balance_result.get('error', 'Unknown error')}")
            self.logger.info("Kraken API connection successful.")

            # Setup Hyperliquid API
            if self.hl_secret:
                try:
                    # Initialize SDK using the private key. The SDK will handle authentication.
                    # We don't need to explicitly derive or pass the public address here for trading functions.
                    self.hl_exchange = Exchange(wallet=self.hl_secret)
                    self.logger.info("Hyperliquid Exchange client initialized successfully.") # Added specific log

                    # Initialize the Info client separately, trapping its specific errors
                    try:
                        self.logger.info("Attempting to initialize Hyperliquid Info client...")
                        self.hl_info = Info(constants.MAINNET_API_URL, skip_ws=True)
                        if self.hl_info: # Add check if Info() might return None on failure
                             self.logger.info("Hyperliquid Info client initialized successfully.")
                        else:
                             self.logger.error("Hyperliquid Info() returned None or False. Setting hl_info to None.")
                             self.hl_info = None # Ensure it's None
                    except Exception as info_e:
                        self.logger.error(f"ERROR during Hyperliquid Info() init: Type={type(info_e).__name__}, Msg={str(info_e)}")
                        self.logger.exception(f"Full traceback for HL Info() init failure:")
                        self.hl_info = None # Ensure it is None on exception

                    # Log the address that will be used for WebSocket state subscriptions
                    if not self.hl_wallet: # Changed back to hl_wallet
                         self.logger.warning("Hyperliquid wallet address not found in config. WebSocket balance/margin data may not be received.")
                    else:
                         # Ensure address format is consistent (e.g., lowercase 0x prefix)
                         self.hl_wallet = self.hl_wallet.lower() # Changed back to hl_wallet
                         if not self.hl_wallet.startswith('0x'): # Changed back to hl_wallet
                             self.hl_wallet = '0x' + self.hl_wallet # Changed back to hl_wallet
                         self.logger.info(f"Hyperliquid SDK initialized. WebSocket state subscription will use config wallet: {self.hl_wallet}") # Changed back to hl_wallet

                except Exception as e:
                    # Explicitly log exception type and message BEFORE the traceback
                    self.logger.error(f"ERROR during Hyperliquid SDK init: Type={type(e).__name__}, Msg={str(e)}")
                    self.logger.exception(f"Full traceback for HL SDK init failure:") # Keep traceback log
                    self.hl_exchange = None # Reset exchange on failure
                    self.hl_info = None # Ensure info client is None too on failure
            else: # This block executes if self.hl_secret was not found/provided
                self.logger.error("Hyperliquid private key not provided in config.")
                self.hl_exchange = None
                self.hl_info = None

            # Add diagnostic logging before the check
            hl_exchange_exists = hasattr(self, 'hl_exchange') and self.hl_exchange is not None
            hl_info_exists = hasattr(self, 'hl_info') and self.hl_info is not None
            kraken_keys_exist = hasattr(self, 'kraken_key') and self.kraken_key and hasattr(self, 'kraken_secret') and self.kraken_secret
            self.logger.info(f"DIAGNOSTIC: Before check - kraken_keys_exist={kraken_keys_exist}, hl_exchange_exists={hl_exchange_exists}, hl_info_exists={hl_info_exists}")
            self.logger.info(f"DIAGNOSTIC: type(hl_exchange)={type(self.hl_exchange)}, bool(hl_exchange)={bool(self.hl_exchange)}")
            self.logger.info(f"DIAGNOSTIC: type(hl_info)={type(self.hl_info)}, bool(hl_info)={bool(self.hl_info)}")

            # We need Kraken keys and HL clients
            if not kraken_keys_exist or not self.hl_exchange or not self.hl_info:
                self.logger.error("Failed to initialize one or more exchange clients/credentials (Kraken Keys, HL Exchange, HL Info). Bot cannot run.")
                raise ConnectionError("Failed to initialize exchange API clients or load credentials.")
            else:
                self.logger.info("Exchange API clients/credentials initialized.")

        
            # --- Initial Balance Check ---
            self.logger.info("Performing initial balance check...")
            # Use existing check_balances method
            balances = self.check_balances()
            if not balances or balances.get('error'):
                # Log error but allow continuation if possible, bot might recover later
                self.logger.error(f"Initial balance check failed or returned error: {balances.get('details', 'Unknown')}. Bot will continue, but may fail trades.")
            else:
                k_bal_str = f"${balances.get('kraken', {}).get('balance', 0):.2f}" if balances.get('kraken', {}).get('success') else "Error"
                h_bal_str = f"${balances.get('hyperliquid', {}).get('available', 0):.2f}" if balances.get('hyperliquid', {}).get('success') else "Error"
                self.logger.info(f"Initial Balances: Kraken ~{k_bal_str}, HL Available Margin ~{h_bal_str}")
                if not balances.get('sufficient', True):
                    self.logger.warning("Initial balances reported as potentially insufficient for minimum trade sizes.")

        except Exception as e:
            self.logger.exception(f"API setup failed: {e}")
            # Re-raise as this is critical
            raise Exception("API setup failed.") from e

    def filter_assets_by_selected_tokens(self):
        """Load specs for selected tokens from TOKEN_SPECIFICATIONS and initialize runtime state in self.assets."""
        self.logger.info(f"Configuring assets. User selected: {self.selected_tokens}, Tier: {self.tier}")
        self.assets = {} # Reset assets dictionary
        self.supported_assets = [] # Reset supported assets list

        if not self.selected_tokens:
            self.logger.warning("No tokens selected by the user. Bot cannot trade.")
            return # No assets to configure

        if not self.token_specifications:
            self.logger.error("TOKEN_SPECIFICATIONS is empty or failed to load. Cannot configure assets.")
            return # Cannot proceed without specs

        valid_selected_tokens = []
        for token in self.selected_tokens:
            token_upper = token.upper() # Ensure uppercase for lookup
            if token_upper in self.token_specifications:
                specs = self.token_specifications[token_upper]

                # Basic validation of specs needed for sizing/trading
                if 'margin_size' not in specs or not isinstance(specs['margin_size'], (int, float)) or specs['margin_size'] <= 0:
                    self.logger.warning(f"Token '{token_upper}' skipped: Missing or invalid 'margin_size' in TOKEN_SPECIFICATIONS.")
                    continue
                if 'kraken_pair' not in specs and not specs.get('hyperliquid_spot', False): # HYPE exception
                     self.logger.warning(f"Token '{token_upper}' skipped: Missing 'kraken_pair' in TOKEN_SPECIFICATIONS (and not HL spot).")
                     continue
                if 'price_precision' not in specs:
                     self.logger.warning(f"Token '{token_upper}' missing 'price_precision', using default 4.")
                     specs['price_precision'] = 4 # Provide a default if missing

                # Initialize runtime state for this asset
                self.assets[token_upper] = {
                    'spec': specs,                      # Store the loaded specifications
                    'in_position': False,               # Currently holding a position?
                    'last_price': None,                 # Last known price (e.g., from Kraken ticker)
                    'ws_funding_rate': None,            # Current funding rate (decimal) from WS/API
                    'ws_predicted_rate': None,          # Predicted funding rate (decimal) from WS/API
                    'websocket_subscribed': False,      # HL WS subscription status (if needed)
                    'hl_best_bid': None,                # For potential limit order placement
                    'hl_best_ask': None,                # For potential limit order placement
                    'position_size_hl_usd': None,       # Stored USD size of HL leg after successful entry
                    'position_size_kraken_usd': None,   # Stored USD size of Kraken leg after successful entry
                    'position_size_hl_qty': None,       # Stored Qty size of HL leg
                    'position_size_kraken_qty': None,   # Stored Qty size of Kraken leg
                    'entry_timestamp': None,            # Time when the position was entered
                    'last_error': None,                 # Track last error related to this asset
                }
                valid_selected_tokens.append(token_upper)
                self.logger.info(f"Configured asset: {token_upper} (Margin: {specs['margin_size']}, Kraken: {specs.get('kraken_pair', 'N/A')})")
            else:
                self.logger.warning(f"Selected token '{token_upper}' not found in TOKEN_SPECIFICATIONS.")

        # Handle Tier 1 constraint - must trade only one token
        if self.tier == 1 and len(valid_selected_tokens) > 1:
            # If multiple valid tokens selected for Tier 1, prioritize based on selection order
            chosen_token_t1 = valid_selected_tokens[0]
            self.logger.warning(f"Tier 1 bot selected multiple valid tokens ({valid_selected_tokens}). Using only the first: {chosen_token_t1}")
            # Keep only the chosen token in self.assets
            self.assets = {chosen_token_t1: self.assets[chosen_token_t1]}
            self.supported_assets = [chosen_token_t1]
        else:
            self.supported_assets = valid_selected_tokens # For Tier 2/3, use all valid selected tokens

        if not self.supported_assets:
            self.logger.error("No valid selected tokens could be configured based on TOKEN_SPECIFICATIONS!")
        else:
            self.logger.info(f"Bot will operate with assets: {self.supported_assets}")


    # --- WebSocket Methods --- (Adapted from original, simplified open)

    def connect_websocket(self):
        """Initiate WebSocket connection in a separate thread."""
        if not self.assets:
            self.logger.warning("No assets configured, skipping WebSocket connection.")
            return
        if self.ws_thread and self.ws_thread.is_alive():
            self.logger.warning("WebSocket manager thread is already running.")
            return
        self.ws_running = True
        self.ws_thread = threading.Thread(target=self._ws_thread_manager, name=f"WSMgr_{self.user_id}")
        self.ws_thread.daemon = True
        self.ws_thread.start()
        self.logger.info("WebSocket manager thread started.")

    def stop_websocket(self):
        """Stop the WebSocket connection and thread."""
        self.logger.info("Stopping WebSocket...")
        self.ws_running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception as e:
                self.logger.error(f"Exception closing WebSocket: {e}")
        if self.ws_thread and self.ws_thread.is_alive():
            self.logger.info("Waiting for WebSocket manager thread to join...")
            self.ws_thread.join(timeout=5)
            if self.ws_thread.is_alive():
                 self.logger.warning("WebSocket manager thread did not join cleanly.")
        self.ws = None
        self.ws_thread = None
        self.logger.info("WebSocket stopped.")

    def _ws_thread_manager(self):
        """Manages WebSocket connection and reconnection attempts."""
        self.ws_reconnect_count = 0
        while self.ws_running and self.ws_reconnect_count < self.max_ws_reconnects:
            try:
                self.logger.info(f"Attempting WebSocket connection (Attempt: {self.ws_reconnect_count + 1})...")
                # Use the WebSocketApp from the original script
                self.ws = websocket.WebSocketApp(
                    self.hl_ws_url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                # run_forever blocks until connection close/error
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except websocket.WebSocketException as wse:
                 self.logger.error(f"WebSocketException in run_forever: {wse}")
            except Exception as e:
                 self.logger.error(f"Unexpected error in WebSocket run_forever: {e}")

            # If loop continues, it means run_forever exited (connection closed/error)
            if self.ws_running: # Only attempt reconnect if shutdown wasn't requested
                self.ws_reconnect_count += 1
                wait_time = min(60, 2 ** self.ws_reconnect_count) # Exponential backoff
                self.logger.warning(f"WebSocket disconnected. Reconnecting in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                 self.logger.info("WebSocket manager exiting because self.running is False.")

        if self.ws_running and self.ws_reconnect_count >= self.max_ws_reconnects:
            self.logger.error("Maximum WebSocket reconnection attempts reached. Stopping WebSocket.")
            self.running = False # Consider stopping the bot if WS fails permanently
            self.stop_websocket() # Ensure cleanup
        elif not self.ws_running:
             self.logger.info("WebSocket manager stopped.")


    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.logger.info("WebSocket connection established.")
        # Wait a moment before subscribing to prevent subscription issues
        time.sleep(1)
        
        # Subscribe to active asset context for each selected token
        for asset in self.assets:
            subscribe_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "activeAssetCtx",
                    "coin": asset
                }
            }
            ws.send(json.dumps(subscribe_msg))
            self.logger.info(f"Sent WebSocket subscription for: {asset}")
            
        # Reset reconnect count on successful connection
        self.ws_reconnect_count = 0

    def on_message(self, ws, message):
        """Handle WebSocket messages"""
        try:
            data = json.loads(message)
            
            # Handle subscription responses
            if 'method' in data and data['method'] == 'subscribe':
                self.logger.info(f"Subscription Response: {data}")
                return
                
            # Handle active asset context updates
            if 'channel' in data and data['channel'] == 'activeAssetCtx':
                ctx = data.get('data', {}).get('ctx', {})
                asset = data.get('data', {}).get('coin')
                
                if ctx and asset in self.assets:
                    if 'funding' in ctx:
                        self.assets[asset]['ws_funding_rate'] = float(ctx['funding'])
                        self.last_funding_rates[asset] = float(ctx['funding']) * 100  # Store as percentage
                        self.logger.info(f"Updated {asset} funding rate from WebSocket: {float(ctx['funding']) * 100:.4f}%")
                        
                    if 'predFunding' in ctx:
                        self.assets[asset]['ws_predicted_rate'] = float(ctx['predFunding'])
                        self.logger.info(f"Updated {asset} predicted funding rate: {float(ctx['predFunding']) * 100:.4f}%")
                        
                    if 'impactPxs' in ctx:
                        self.assets[asset]['hl_best_bid'] = float(ctx['impactPxs'][0])
                        self.assets[asset]['hl_best_ask'] = float(ctx['impactPxs'][1])
                        
                    if 'premium' in ctx:
                        self.assets[asset]['premium'] = float(ctx['premium'])
                        
                    if 'oraclePx' in ctx:
                        self.assets[asset]['oracle_px'] = float(ctx['oraclePx'])
                        
                    # Check conditions after receiving new data
                    self.check_entry_conditions(asset)
                    
        except Exception as e:
            self.logger.error(f"Error in WebSocket message handler: {e}")

    def on_error(self, ws, error):
        """Callback for WebSocket errors."""
        # Log specific WebSocket errors if possible
        if isinstance(error, websocket.WebSocketException):
             self.logger.error(f"WebSocket Error: {error}")
        else:
             self.logger.error(f"Generic WebSocket Error: {type(error).__name__} - {error}")
        # Reconnection is handled by the _ws_thread_manager loop exiting run_forever

    def on_close(self, ws, close_status_code, close_msg):
        """Callback when WebSocket connection is closed."""
        self.logger.warning(f"WebSocket connection closed. Code: {close_status_code}, Message: {close_msg}")
        # Reset subscription status for all assets (will resubscribe on reconnect)
        for asset in self.assets:
            self.assets[asset]['websocket_subscribed'] = False # Assuming this flag is used elsewhere
        # Reconnection is handled by the _ws_thread_manager loop


    # --- WebSocket Message Processing ---
    
    def start_message_processor(self):
        """Start the background thread for processing WebSocket messages off the queue."""
        processor_thread = threading.Thread(target=self.process_websocket_messages, name=f"WSProc_{self.user_id}")
        processor_thread.daemon = True
        processor_thread.start()
        self.logger.info("WebSocket message processor thread started.")
    
    def process_websocket_messages(self):
        """Continuously processes messages from the WebSocket message queue."""
        self.logger.info("WebSocket message processor running.")
        while self.running:
            try:
                # Get message from queue with timeout to allow checking self.running
                message = self.ws_message_queue.get(timeout=1)
                self.process_single_ws_message(message) # Process the dequeued message
                self.ws_message_queue.task_done() # Mark task as complete
            except queue.Empty:
                # Queue is empty, loop continues checking self.running
                continue
            except Exception as e:
                # Log error but continue processing other messages
                self.logger.exception(f"Error processing WebSocket message from queue: {e}")
                # Avoid tight spin on continuous error
                time.sleep(0.1)
        self.logger.info("WebSocket message processor stopped.")


    def process_single_ws_message(self, message):
        """Process a single WebSocket message from the queue."""
        try:
            # Decode if necessary
            decoded_message = message
            if isinstance(message, (memoryview, bytes)):
                decoded_message = (message.tobytes() if isinstance(message, memoryview) else message).decode('utf-8')

            data = json.loads(decoded_message)
            msg_type = data.get('channel') # Use 'channel' as the key for message type

            if msg_type == 'activeAssetCtx':
                ctx_data = data.get('data', {}).get('ctx', {})
                asset = ctx_data.get('coin') # Assuming 'coin' is the key for asset name

                # --- BEGIN ADDITION: Log raw ctx_data for debugging ---
                if asset:
                    self.logger.debug(f"DEBUG activeAssetCtx for {asset}: {ctx_data}")
                # --- END ADDITION ---

                if asset and asset in self.assets:
                    current_rate_decimal = None
                    predicted_rate_decimal = None
                    current_rate_percent = None

                    try:
                        if 'funding' in ctx_data:
                            current_rate_decimal = float(ctx_data['funding'])
                            current_rate_percent = current_rate_decimal * 100.0
                        if 'predFunding' in ctx_data:
                            predicted_rate_decimal = float(ctx_data['predFunding'])
                            
                        # Thread-safe update of shared state
                        with self.lock:
                            if current_rate_decimal is not None:
                                self.assets[asset]["ws_funding_rate"] = current_rate_decimal
                                self.last_funding_rates[asset] = current_rate_percent # Store % for checks
                                self.logger.debug(f"{asset}: Updated Current Rate = {current_rate_percent:.6f}% from activeAssetCtx")
                            if predicted_rate_decimal is not None:
                                self.assets[asset]["ws_predicted_rate"] = predicted_rate_decimal
                                self.logger.debug(f"{asset}: Updated Predicted Rate = {predicted_rate_decimal * 100.0:.6f}% from activeAssetCtx")

                        # Trigger condition checks AFTER updating rates
                        # Ensure lock is released before calling potentially blocking functions
                        if not self.assets[asset]['in_position']:
                            self.check_entry_conditions(asset)
                        else:
                            self.check_exit_conditions(asset)

                    except (ValueError, TypeError) as e:
                        self.logger.warning(f"Error parsing funding data for {asset} from activeAssetCtx: {e} - Data: {ctx_data}")
                else:
                     if asset: self.logger.debug(f"Received activeAssetCtx for unmanaged asset: {asset}")

            elif msg_type == 'allMids':
                # Keep existing allMids logic (if any useful data is still parsed here)
                # NOTE: This might become redundant for funding rates if activeAssetCtx is reliable
                mids_data = data.get('data')
                if mids_data and isinstance(mids_data, dict) and 'mids' in mids_data:
                     mids = mids_data.get('mids', {}) # Assuming mids is a dict {asset: price_str}
                     if isinstance(mids, dict):
                          with self.lock:
                               # Update mid prices if needed elsewhere
                               # self.mid_prices.update(mids)
                               pass # Placeholder if mid prices aren't used directly
                     else:
                          self.logger.warning(f"Unexpected 'mids' format in allMids: {type(mids)}")


            elif msg_type == 'l2Book':
                # Keep existing l2Book logic (if needed)
                l2_data = data.get('data')
                if l2_data and isinstance(l2_data, dict) and 'coin' in l2_data and 'levels' in l2_data:
                    coin = l2_data['coin']
                    levels = l2_data['levels'] # List of [price, size] pairs for bids and asks
                    with self.lock:
                         if coin in self.assets:
                              # Example: Storing the top bid/ask (optional)
                              bids = [lvl for lvl in levels[0]] # Assuming levels[0] are bids
                              asks = [lvl for lvl in levels[1]] # Assuming levels[1] are asks
                              if bids: self.assets[coin]['hl_best_bid'] = float(bids[0][0])
                              if asks: self.assets[coin]['hl_best_ask'] = float(asks[0][0])
                    self.logger.debug(f"Received L2 book update for {coin}")

            elif msg_type == 'userEvents':
                # Keep existing userEvents logic (for fills etc.)
                user_data = data.get('data')
                if user_data and isinstance(user_data, dict) and 'userEvents' in user_data:
                    for event in user_data['userEvents']:
                        event_type = event.get('type')
                        if event_type == 'fill':
                            fill_details = event.get('details')
                            if fill_details:
                                asset = fill_details.get('coin')
                                side = fill_details.get('side')
                                price = fill_details.get('price')
                                size = fill_details.get('sz')
                                self.logger.info(f"HL FILL EVENT: Asset={asset}, Side={side}, Price={price}, Size={size}")
                                # Potentially update internal state based on fill
                        elif event_type == 'order':
                            order_details = event.get('details')
                            self.logger.info(f"HL ORDER EVENT: {order_details}")
                        # Add handling for other user events if needed

            elif msg_type == 'userFills':
                 # Keep existing userFills logic (might be redundant with userEvents)
                 fill_data = data.get('data')
                 if fill_data and isinstance(fill_data, dict) and 'fills' in fill_data:
                      for fill in fill_data['fills']:
                         asset = fill.get('coin'); side = fill.get('side'); price = fill.get('price'); size = fill.get('sz')
                         self.logger.info(f"HL USER FILL (REST-like): Asset={asset}, Side={side}, Price={price}, Size={size}")

            elif msg_type == 'webData2':
                 # Keep existing webData2 logic (for margin, positions)
                 web_data = data.get('data')
                 if web_data and isinstance(web_data, dict):
                     margin_summary = web_data.get('marginSummary')
                     if margin_summary:
                          with self.lock: self.hl_margin_summary = margin_summary
                          self.logger.debug(f"HL Margin Summary Update via webData2")

                     position_updates = web_data.get('clearinghouseState', {}).get('assetPositions')
                     if position_updates:
                         with self.lock:
                             for pos_data in position_updates:
                                 pos_details = pos_data.get('position')
                                 if pos_details:
                                     asset = pos_details.get('coin')
                                     if asset in self.assets:
                                         szi = pos_details.get('szi')
                                         self.assets[asset]['hl_position_size_ws'] = float(szi) if szi else 0.0
                                         # Update other position details if needed
                         self.logger.debug(f"HL Position Updates via webData2")

            elif msg_type == 'subscriptionResponse':
                self.logger.info(f"Subscription Response: {data.get('data')}")
            elif msg_type == 'error':
                self.logger.error(f"WebSocket Error Message: {data.get('data')}")
            # else:
            #     self.logger.debug(f"Ignoring unknown WS message type: {msg_type}")

        except json.JSONDecodeError:
            # Use the original raw message in the error log if decoding fails
            log_msg = message if isinstance(message, str) else repr(message)
            self.logger.error(f"Failed to decode JSON message: {log_msg[:250]}")
        except Exception as e:
            log_msg = message if isinstance(message, str) else repr(message)
            self.logger.exception(f"Error processing WebSocket message: {log_msg[:250]} - Error: {e}")


    # --- Core Trading Logic ---

    def check_entry_conditions(self, asset: str):
        """Check if an asset meets the percentile criteria for potential entry."""
        if not self.running or asset not in self.assets or self.assets[asset].get('in_position'):
            return # Bot stopped, asset not configured, or already in position

        # Use the current funding rate stored from WS/API
        current_rate_percent = self.last_funding_rates.get(asset)
        asset_hist_percentiles = self.historical_percentiles.get(asset)
        entry_perc_value = self.entry_percentiles.get(self.entry_strategy) # e.g., 60, 75

        if current_rate_percent is None:
            self.logger.debug(f"Entry Check {asset}: Skipping, current rate unavailable.")
            return
        if asset_hist_percentiles is None:
             self.logger.debug(f"Entry Check {asset}: Skipping, historical percentiles unavailable.")
             return
        if entry_perc_value is None:
            self.logger.error(f"Entry Check {asset}: Skipping, invalid entry strategy '{self.entry_strategy}' - percentile value not found.")
            return
            
        entry_perc_key = str(entry_perc_value) # Key in historical_percentiles is string '60', '75' etc.
        if entry_perc_key not in asset_hist_percentiles:
            self.logger.warning(f"Entry Check {asset}: Skipping, required entry percentile '{entry_perc_key}' not found in calculated historical percentiles.")
            return # Specific percentile wasn't calculated or loaded

        entry_threshold_rate = asset_hist_percentiles[entry_perc_key] # The rate at the Nth percentile

        self.logger.debug(f"Entry Check {asset} (Strat: '{self.entry_strategy}'): Current Rate={current_rate_percent:.4f}%, Threshold ({entry_perc_key}th)={entry_threshold_rate:.4f}%")

        # --- Entry Condition ---
        # Rate must be positive AND exceed the threshold percentile rate
        if current_rate_percent > 0 and current_rate_percent > entry_threshold_rate:
            self.logger.info(f"✅ Entry condition met for {asset} (Rate: {current_rate_percent:.4f}% > Threshold: {entry_threshold_rate:.4f}%)")
            # Add to candidates if not already there, triggers evaluation
            if asset not in self.entry_candidates:
                self.entry_candidates.add(asset)
                # Trigger evaluation immediately after adding a candidate
                # Use execute_with_cooldown to handle potential errors/rate limits in evaluation
                self.execute_with_cooldown(self.evaluate_and_execute_trades)
        else:
             # Remove from candidates if conditions are no longer met
             if asset in self.entry_candidates:
                  self.logger.info(f"Entry condition no longer met for {asset}. Removing from candidates.")
                  self.entry_candidates.discard(asset)

    def evaluate_and_execute_trades(self):
        """Evaluate candidate assets and execute trades based on tier logic and sizing."""
        # Prevent concurrent evaluations which could lead to oversizing
        if hasattr(self, '_evaluating_trades') and self._evaluating_trades:
            self.logger.debug("Evaluation already in progress, skipping.")
            return
        self._evaluating_trades = True

        try:
            if not self.running or not self.entry_candidates:
                self.logger.debug("No entry candidates to evaluate.")
                return

            self.logger.info(f"Evaluating candidates: {list(self.entry_candidates)}")

            # Re-check candidate validity before proceeding (conditions might change fast)
            valid_candidates = []
            assets_to_remove = set() # Track assets whose conditions are no longer met
            for asset in list(self.entry_candidates): # Use list() for safe iteration
                if asset not in self.assets or self.assets[asset].get('in_position'):
                    assets_to_remove.add(asset)
                    continue # Should not happen if check_entry_conditions is correct, but safety first

                current_rate = self.last_funding_rates.get(asset)
                asset_percentiles = self.historical_percentiles.get(asset)
                entry_perc_val = self.entry_percentiles.get(self.entry_strategy)

                if current_rate is None or asset_percentiles is None or entry_perc_val is None:
                    self.logger.warning(f"Data missing for candidate {asset}, removing.")
                    assets_to_remove.add(asset); continue

                entry_perc_key = str(entry_perc_val)
                if entry_perc_key not in asset_percentiles:
                     self.logger.warning(f"Required percentile {entry_perc_key} missing for candidate {asset}, removing.")
                     assets_to_remove.add(asset); continue

                entry_threshold = asset_percentiles[entry_perc_key]
                if current_rate > 0 and current_rate > entry_threshold:
                    valid_candidates.append(asset) # Still meets criteria
                else:
                    self.logger.info(f"Candidate {asset} no longer meets entry criteria, removing.")
                    assets_to_remove.add(asset) # Condition no longer met

            # Remove invalidated candidates
            for asset in assets_to_remove:
                self.entry_candidates.discard(asset)

            if not valid_candidates:
                self.logger.info("No valid candidates remain after re-check.")
                return

            # --- Balance Check ---
            balances = self.check_balances() # Use existing balance check
            if not balances or balances.get('error'):
                self.logger.error(f"Cannot execute trades: Balance check failed or returned error: {balances.get('details')}")
                # Do not clear candidates, might recover on next check
                return
            if not balances.get('sufficient'):
                 self.logger.warning(f"Cannot execute trades: Balances reported as insufficient.")
                 # Do not clear candidates, maybe temporary
                 return

            # --- Calculate Sizes ---
            # Use the dynamic sizing logic based on tier, rates, balances
            trade_sizes_usd = self._calculate_position_sizes(balances, valid_candidates)

            if not trade_sizes_usd:
                self.logger.warning("Position sizing resulted in no valid trades (e.g., below minimums).")
                # Clear candidates as sizing failed with current balances/rates
                self.entry_candidates.clear()
                return

            self.logger.info(f"Calculated Trade Sizes (USD): {trade_sizes_usd}")

            # --- Execute Trades ---
            executed_successfully = set()
            for asset, sizes in trade_sizes_usd.items():
                if asset in self.active_positions: # Double check not already in position
                    self.logger.warning(f"Skipping execution for {asset}, already marked as in position.")
                    continue

                hl_usd = sizes['hl']
                kraken_usd = sizes['kraken']

                # Check minimums again just before execution
                if hl_usd < self.HL_MIN_ORDER_USD or kraken_usd <= 0: # Kraken leg needs some value
                    self.logger.warning(f"Skipping {asset}: Calculated size H=${hl_usd:.2f}/K=${kraken_usd:.2f} below minimums or invalid.")
                    continue

                # Call the actual position entry function (from original script, adapted)
                # Pass USD sizes, let enter_positions handle price fetching and qty conversion
                success = self.enter_positions(asset, hl_usd, kraken_usd)

                if success:
                    self.logger.info(f"✅ Trade entry successfully initiated for {asset}.")
                    # State is updated within enter_positions after confirmed fills
                    executed_successfully.add(asset)
                else:
                    self.logger.error(f"❌ Trade entry failed for {asset}.")
                    # Keep as candidate? Maybe, allow retry on next cycle.
                    # Consider adding asset-specific cooldown on failure.
                    self.assets[asset]['last_error'] = f"Entry failed at {datetime.now()}"

            # Clear candidates that were processed (successfully or not)
            # This prevents immediate re-evaluation if rates fluctuate slightly
            self.entry_candidates.difference_update(trade_sizes_usd.keys())
            
        except Exception as e:
            self.logger.exception(f"Critical error during trade evaluation/execution: {e}")
            # Consider clearing candidates in case of critical error to prevent loops
            # self.entry_candidates.clear()
        finally:
            self._evaluating_trades = False # Release the lock


    def _calculate_position_sizes(self, balances: Dict, tokens_to_trade: List[str]) -> Dict[str, Dict[str, float]]:
        """Calculates position sizes in USD based on balances, tier, rates, and minimums."""
        final_sizes = {} # { 'BTC': {'hl': 15.0, 'kraken': 45.0}, ... }

        # --- Get Usable Balances ---
        kraken_total = balances.get('kraken', {}).get('balance', 0.0)
        # Use 'available' margin from HL as it reflects withdrawable/usable for new positions
        hl_total_available = balances.get('hyperliquid', {}).get('available', 0.0)

        kraken_usable = max(0.0, kraken_total - self.EXCHANGE_SAFETY_BUFFER_USD)
        hl_usable = max(0.0, hl_total_available - self.EXCHANGE_SAFETY_BUFFER_USD)

        # Total capital available for the *entire* arbitrage position (HL short + Kraken long)
        # Limited by the smaller side considering margin requirements
        # Example: If HL can support $20 short and Kraken $100 long (3x margin), total trade value is limited by HL.
        # We need to consider margin for each potential token.

        if hl_usable < self.HL_MIN_ORDER_USD:
             self.logger.warning(f"Insufficient HL usable balance ${hl_usable:.2f} (Min: ${self.HL_MIN_ORDER_USD}). Cannot calculate sizes.")
             return {}
        if kraken_usable <= 0:
             self.logger.warning(f"Insufficient Kraken usable balance ${kraken_usable:.2f}. Cannot calculate sizes.")
             return {}

        self.logger.info(f"Calculating sizes with Usable Balances: Kraken=${kraken_usable:.2f}, HL=${hl_usable:.2f}")

        # Filter candidates to only those with positive rates (arbitrage relies on positive funding)
        positive_rate_tokens = [
            t for t in tokens_to_trade
            if t in self.last_funding_rates and self.last_funding_rates[t] > 0 and t in self.assets
        ]
        if not positive_rate_tokens:
            self.logger.warning("No candidates meet criteria (positive rate).")
            return {}

        # --- Determine Allocation ---
        allocations: Dict[str, float] = {} # { 'BTC': 0.6, 'ETH': 0.4 } - Fraction of total capital

        if self.tier == 1 or len(positive_rate_tokens) == 1:
            # Tier 1 or only one candidate: Allocate 100% to the best (or only) one
            best_token = max(positive_rate_tokens, key=lambda t: self.last_funding_rates.get(t, -1))
            allocations = {best_token: 1.0}
            self.logger.info(f"Tier {self.tier} allocation: 100% to {best_token}")
        else:
            # Tier 2/3: Allocate proportionally to positive funding rates
            rate_sum = sum(self.last_funding_rates[token] for token in positive_rate_tokens)
        if rate_sum <= 0:
                self.logger.warning("Sum of positive rates is zero or negative. Cannot allocate.")
                # Fallback: Allocate to the single highest rate token
                best_token = max(positive_rate_tokens, key=lambda t: self.last_funding_rates.get(t, -1))
                allocations = {best_token: 1.0}
                self.logger.info(f"Rate sum issue. Allocating 100% to highest rate: {best_token}")
        else:
                allocations = {
                    token: self.last_funding_rates[token] / rate_sum
                    for token in positive_rate_tokens
                }
                alloc_str = ", ".join([f"{t}: {f*100:.1f}%" for t, f in allocations.items()])
                self.logger.info(f"Tier {self.tier} allocation: {alloc_str}")

        # --- Calculate Ideal Sizes per Token based on Allocation & Margin ---
        initial_sizes: Dict[str, Dict[str, float]] = {}
        total_ideal_hl_needed = 0.0
        total_ideal_kraken_needed = 0.0

        for token, alloc_frac in allocations.items():
            if token not in self.assets or 'spec' not in self.assets[token]:
                self.logger.warning(f"Specs missing for allocated token {token}, skipping.")
                continue
            margin = self.assets[token]['spec']['margin_size']

            # Determine max capital this token *could* use based on HL vs Kraken limits & margin
            # Max HL short size supported by HL balance: hl_usable
            # Max Kraken long size supported by Kraken balance: kraken_usable
            # Max HL short size supported by Kraken balance (considering margin): kraken_usable / margin
            # Max Kraken long size supported by HL balance (considering margin): hl_usable * margin

            # The limiting factor for the HL short size is min(hl_usable, kraken_usable / margin)
            max_hl_possible = min(hl_usable, kraken_usable / margin)
            # The limiting factor for the Kraken long size is min(kraken_usable, hl_usable * margin)
            max_kraken_possible = min(kraken_usable, hl_usable * margin)

            # Ideal total capital based on combined balances, scaled by allocation fraction
            # This interpretation might be complex. Simpler: Allocate usable balance proportionally.
            # Let's allocate the *minimum* of the usable balances proportionally, adjusted by margin.
            # Effective total capital limit for this token's allocation = min(hl_usable, kraken_usable / margin)

            token_hl_budget = hl_usable * alloc_frac
            token_kraken_budget = kraken_usable * alloc_frac

            # Calculate ideal HL size based on margin and Kraken budget
            ideal_hl_from_kraken = token_kraken_budget / margin
            # Calculate ideal Kraken size based on margin and HL budget
            ideal_kraken_from_hl = token_hl_budget * margin

            # The actual HL size is limited by its own budget and what Kraken can support
            calc_hl = min(token_hl_budget, ideal_hl_from_kraken)
            # The actual Kraken size is limited by its own budget and what HL requires
            calc_kr = min(token_kraken_budget, ideal_kraken_from_hl)

            # Ensure ratio is maintained: kr_size should be hl_size * margin
            # Adjust the smaller leg to match the ratio based on the larger leg
            if calc_hl * margin > calc_kr: # HL leg is proportionally larger
                 calc_hl = calc_kr / margin # Reduce HL to match Kraken support
            else: # Kraken leg is proportionally larger (or equal)
                 calc_kr = calc_hl * margin # Reduce Kraken to match HL requirement

            # Safety check against overall usable limits
            calc_hl = min(calc_hl, hl_usable)
            calc_kr = min(calc_kr, kraken_usable)


            initial_sizes[token] = {'hl': calc_hl, 'kraken': calc_kr}
            total_ideal_hl_needed += calc_hl
            total_ideal_kraken_needed += calc_kr

            self.logger.debug(f"  Initial Calc {token}: H=${calc_hl:.2f}, K=${calc_kr:.2f} (AllocFrac: {alloc_frac:.2f})")


        # --- Adjust Sizes if Total Exceeds Usable & Check Minimums ---
        hl_scale = 1.0 if total_ideal_hl_needed <= hl_usable else hl_usable / total_ideal_hl_needed
        kraken_scale = 1.0 if total_ideal_kraken_needed <= kraken_usable else kraken_usable / total_ideal_kraken_needed

        # Use the *minimum* scale factor to ensure neither exchange is overallocated
        scale_factor = min(hl_scale, kraken_scale)

        if scale_factor < 1.0:
            self.logger.info(f"Total calculated sizes exceed usable balance. Scaling by {scale_factor:.4f}")

        min_hl_fail = False
        scaled_sizes: Dict[str, Dict[str, float]] = {}
        for token, sizes in initial_sizes.items():
            scaled_hl = sizes['hl'] * scale_factor
            scaled_kr = sizes['kraken'] * scale_factor

            # Re-check margin ratio after scaling
            margin = self.assets[token]['spec']['margin_size']
            if abs(scaled_kr - scaled_hl * margin) > 0.01: # Allow tiny float deviation
                self.logger.warning(f"Margin ratio mismatch after scaling for {token}. H={scaled_hl:.2f}, K={scaled_kr:.2f}, Margin={margin}")
                # Adjust Kraken size to strictly match scaled HL size and margin
                scaled_kr = scaled_hl * margin

            scaled_sizes[token] = {'hl': scaled_hl, 'kraken': scaled_kr}
            self.logger.debug(f"  Scaled Calc {token}: H=${scaled_hl:.2f}, K=${scaled_kr:.2f}")

            # Check HL minimum order size for multi-token tiers
            if self.tier > 1 and len(allocations) > 1 and scaled_hl < self.HL_MIN_ORDER_USD:
                min_hl_fail = True
                self.logger.warning(f"Scaled HL size for {token} (${scaled_hl:.2f}) is below minimum ${self.HL_MIN_ORDER_USD}.")

        # --- Pivot Logic (for Tier 2/3 if any allocation failed minimum) ---
        if min_hl_fail:
            self.logger.warning("Minimum HL size not met for at least one token in multi-token allocation. Pivoting to highest rate token.")
            best_token = max(positive_rate_tokens, key=lambda t: self.last_funding_rates.get(t, -1))

            if best_token not in self.assets or 'spec' not in self.assets[best_token]:
                 self.logger.error(f"Cannot pivot: Specs missing for highest rate token {best_token}.")
                 return {}
            margin = self.assets[best_token]['spec']['margin_size']

            # Recalculate using *total* usable balances for the single best token
            # Max HL size limited by min(hl_usable, kraken_usable / margin)
            # Max Kraken size limited by min(kraken_usable, hl_usable * margin)
            pivot_hl = min(hl_usable, kraken_usable / margin)
            pivot_kr = min(kraken_usable, hl_usable * margin)

            # Ensure ratio: kr = hl * margin
            if pivot_hl * margin > pivot_kr:
                 pivot_hl = pivot_kr / margin
            else:
                 pivot_kr = pivot_hl * margin

            if pivot_hl < self.HL_MIN_ORDER_USD:
                self.logger.error(f"Pivot failed: Calculated HL size ${pivot_hl:.2f} for best token {best_token} is still below minimum.")
                return {} # Cannot execute even the best token

            final_sizes = {best_token: {'hl': pivot_hl, 'kraken': pivot_kr}}
            self.logger.info(f"Pivoted Size for {best_token}: H=${pivot_hl:.2f}, K=${pivot_kr:.2f}")
            return final_sizes

        # --- Final Check & Return (No Pivot Needed or Tier 1) ---
        for token, sizes in scaled_sizes.items():
            hl_s, kr_s = sizes['hl'], sizes['kraken']
            # Final check for minimums and positive value
            if hl_s >= self.HL_MIN_ORDER_USD and kr_s > 0:
                final_sizes[token] = {'hl': hl_s, 'kraken': kr_s}
                self.logger.info(f"  Final Size {token}: H=${hl_s:.2f}, K=${kr_s:.2f}")
            else:
                self.logger.warning(f"  Skipping {token}: Final size H=${hl_s:.2f}, K=${kr_s:.2f} failed minimum/validity check.")

        return final_sizes


    def check_exit_conditions(self, asset: str):
        """Check standard percentile exit AND 'default'/'exit_abraxas' time/predicted rate exit."""
        if not self.running or asset not in self.assets or not self.assets[asset].get('in_position'):
            return # Bot stopped, asset not configured, or not in position

        exit_strategy = self.exit_strategy # User's chosen strategy name ('50', 'exit_abraxas', etc.)
        asset_data = self.assets[asset]
                    
        # --- 1. 'Default' / 'exit_abraxas' Predicted Rate Exit Check ---
        # These strategies use the same logic: exit if predicted rate < 0 near hour end.
        if exit_strategy in ['default', 'exit_abraxas']:
            # Check time window
            now = datetime.now(self.est_tz) # Use consistent timezone
            time_to_hour_end = 60 - now.minute
            in_exit_window = time_to_hour_end <= self.exit_time_threshold

            if in_exit_window:
                self.logger.debug(f"Exit Check ({exit_strategy}) {asset}: Within time window ({time_to_hour_end}m left).")
                predicted_rate_decimal = asset_data.get("ws_predicted_rate") # Rate for next hour (decimal)

                if predicted_rate_decimal is not None:
                    predicted_rate_percent = predicted_rate_decimal * 100
                    self.logger.info(f"Exit Check ({exit_strategy}) {asset}: Predicted rate = {predicted_rate_percent:.6f}%")
                    # --- Exit Condition ---
                    if predicted_rate_decimal < 0:
                        self.logger.info(f"⛔ Exit Trigger ({exit_strategy}) for {asset}: Predicted NEGATIVE rate ({predicted_rate_percent:.4f}%) within {self.exit_time_threshold}m window.")
                        # Use execute_with_cooldown for the exit action
                        self.execute_with_cooldown(self.exit_positions, asset)
                        return # Exit triggered, no need for further checks
                    else:
                         self.logger.debug(f"Exit Check ({exit_strategy}) {asset}: Predicted rate non-negative.")
                else:
                    # This is critical - if predicted rate is needed but unavailable, the strategy fails.
                    self.logger.warning(f"Exit Check ({exit_strategy}) {asset}: Predicted rate unavailable. Cannot perform check.")
            # else:
                 # self.logger.debug(f"Exit Check ({exit_strategy}) {asset}: Outside time window ({time_to_hour_end}m left).")
            return # For default/abraxas, only time/predicted matters. Do not check percentiles.


        # --- 2. Standard Percentile Exit Check ---
        # Only proceed if the strategy corresponds to a percentile value
        exit_percentile_value = self.exit_percentiles.get(exit_strategy)
        if exit_percentile_value is not None: # Strategy is percentile-based (e.g., '50', '35')
            current_rate_percent = self.last_funding_rates.get(asset) # Uses CURRENT rate %
            asset_hist_percentiles = self.historical_percentiles.get(asset)

            if current_rate_percent is None:
                self.logger.debug(f"Exit Check ({exit_strategy}) {asset}: Skipping, current rate unavailable.")
                return
            if asset_hist_percentiles is None:
                 self.logger.debug(f"Exit Check ({exit_strategy}) {asset}: Skipping, historical percentiles unavailable.")
                 return

            exit_perc_key = str(exit_percentile_value) # e.g., '50', '35'
            if exit_perc_key not in asset_hist_percentiles:
                 self.logger.warning(f"Exit Check ({exit_strategy}) {asset}: Skipping, required exit percentile '{exit_perc_key}' not found in calculated historical percentiles.")
                 return

            exit_threshold_rate = asset_hist_percentiles[exit_perc_key] # Rate at the Nth percentile

            self.logger.debug(f"Exit Check {asset} (Strat: '{exit_strategy}'): Current Rate={current_rate_percent:.4f}%, Threshold ({exit_perc_key}th)={exit_threshold_rate:.4f}%")

            # --- Exit Condition ---
            if current_rate_percent <= exit_threshold_rate:
                self.logger.info(f"⛔ Exit Trigger ({exit_strategy}) for {asset}: Current Rate {current_rate_percent:.4f}% <= Threshold {exit_threshold_rate:.4f}%")
                self.execute_with_cooldown(self.exit_positions, asset)
                return # Exit triggered
            else:
             # This case should ideally not be reached if load_custom_settings validates correctly
             self.logger.error(f"Exit Check {asset}: Encountered exit strategy '{exit_strategy}' which has no defined logic (should be None or a percentile).")
                
    
    # --- Position Entry/Exit Execution --- (Wrappers around original order logic)

    def enter_positions(self, asset: str, hl_target_usd: float, kraken_target_usd: float) -> bool:
        """
        Orchestrates the entry into a position for a given asset, ensuring atomicity.
        Calculates QTY, places Kraken BUY, then HL SELL SHORT using Kraken's fill qty.
        Handles failures and attempts to revert.
        
        Args:
            asset: The asset symbol (e.g., 'BTC').
            hl_target_usd: The target USD value for the Hyperliquid short position.
            kraken_target_usd: The target USD value for the Kraken long position.
            
        Returns:
            True if both legs were successfully entered and confirmed, False otherwise.
        """
        self.logger.info(f"--- Attempting Position Entry for {asset} --- Target Sizes: HL=${hl_target_usd:.2f}, Kraken=${kraken_target_usd:.2f}")
        if asset not in self.assets:
            self.logger.error(f"Entry ({asset}): Asset configuration not found.")
            return False
        asset_data = self.assets[asset]
        
        # --- Ensure not already in position ---
        if asset_data.get('in_position', False):
            self.logger.warning(f"Entry ({asset}): Already marked as in position, skipping entry.")
            return False
        
        # --- Get Price & Calculate Initial QTY ---
        # Use Kraken ticker as primary price source for initial QTY calc
        kraken_bid, kraken_ask = self.get_kraken_ticker(asset)
        if kraken_ask is None or kraken_ask <= 0:
            self.logger.error(f"Entry ({asset}): Failed to get valid Kraken ask price (${kraken_ask}). Cannot calculate quantities.")
            return False
        entry_price_estimate = kraken_ask # Estimate buy price
        asset_data['last_price'] = entry_price_estimate # Store for reference

        # Calculate Kraken QTY based on its USD target and estimated price
        # HL QTY will be based on Kraken's actual fill
        kraken_qty_target = kraken_target_usd / entry_price_estimate
        if kraken_qty_target <= 0:
             self.logger.error(f"Entry ({asset}): Calculated Kraken Qty ({kraken_qty_target:.8f}) is not positive.")
        return False
        
        # Check against minimum USD size for HL (as a proxy for Kraken too)
        est_kraken_usd_value = kraken_qty_target * entry_price_estimate
        if est_kraken_usd_value < self.HL_MIN_ORDER_USD:
            self.logger.warning(f"Entry ({asset}): Estimated Kraken order value ${est_kraken_usd_value:.2f} is below minimum ${self.HL_MIN_ORDER_USD}. Skipping.")
            return False
        
        self.logger.info(f"Entry ({asset}): Estimated Kraken Qty = {kraken_qty_target:.8f} based on Ask Price ${entry_price_estimate:.4f}")

        # --- Place Kraken BUY Order ---
        kraken_filled_qty = None
        kraken_avg_price = None
        kraken_success = False
        kraken_txid = None # Store txid for potential fill query

        try:
            # place_kraken_order now handles limit orders, retries, fill wait
            kraken_success, kraken_avg_price = self.place_kraken_order(asset=asset, is_entry=True, position_size=kraken_qty_target)
            kraken_txid = asset_data.get('current_kraken_order_id') # Get txid set by place_kraken_order

            if kraken_success and kraken_avg_price is not None:
                 # CRITICAL: Re-query fill info to get exact executed quantity
                 if kraken_txid:
                     query_attempts = 5
                     for i in range(query_attempts):
                         time.sleep(1.5) # Give Kraken time to update status/fills
                         fill_info = self.get_kraken_order_fill_info(asset, kraken_txid)
                         if fill_info:
                             # Check status is closed and vol_exec is valid
                             if fill_info.get('status') == 'closed':
                                 fetched_filled_qty = fill_info.get('vol_exec')
                                 if fetched_filled_qty is not None and fetched_filled_qty > 1e-9:
                                      kraken_filled_qty = fetched_filled_qty
                                      # Use the avg_price returned by wait_for_fill if possible, otherwise from query
                                      kraken_avg_price = kraken_avg_price if kraken_avg_price is not None else fill_info.get('avg_price')
                                      self.logger.info(f"Entry ({asset}): Kraken BUY order confirmed filled. Qty={kraken_filled_qty:.8f}, Avg Price=${kraken_avg_price:.4f if kraken_avg_price else 'N/A'}")
                                      break # Fill confirmed
                                 else:
                                      self.logger.error(f"Entry ({asset}): Kraken order {kraken_txid} closed but reported zero/invalid filled volume: {fetched_filled_qty}. Treating as failure.")
                                      kraken_success = False; break
                             elif fill_info.get('status') in ['open', 'pending'] and i < query_attempts - 1:
                                 self.logger.warning(f"Entry ({asset}): Kraken order {kraken_txid} status is {fill_info.get('status')} after place_kraken_order success. Re-checking ({i+1}/{query_attempts})...")
                                 continue # Retry query
                             else: # Terminal status other than closed or still open after retries
                                 self.logger.error(f"Entry ({asset}): Kraken order {kraken_txid} has unexpected status '{fill_info.get('status')}' after supposed success/fill.")
                                 kraken_success = False; break
                         else:
                             self.logger.error(f"Entry ({asset}): Failed to get fill info for Kraken order {kraken_txid} (Attempt {i+1}/{query_attempts}).")
                             if i == query_attempts - 1: kraken_success = False # Failed after retries
                     if kraken_filled_qty is None: # Loop finished without confirming fill
                         self.logger.error(f"Entry ({asset}): Could not confirm filled quantity for Kraken order {kraken_txid} after {query_attempts} checks. Treating as failure.")
                         kraken_success = False
                 else:
                     self.logger.error(f"Entry ({asset}): Kraken TxID missing after supposedly successful place_kraken_order call.")
                     kraken_success = False # Treat as failure
            else:
                 # place_kraken_order failed (logged internally)
                 self.logger.error(f"Entry ({asset}): place_kraken_order (BUY) reported failure.")
                 kraken_success = False

        except Exception as e:
            self.logger.exception(f"Entry ({asset}): Exception during Kraken BUY order placement/check: {e}")
            kraken_success = False

        # --- Place HL SELL SHORT Order (ONLY if Kraken leg succeeded and fill qty confirmed) ---
        hl_success = False
        hl_avg_price = None # place_hl_order doesn't reliably return avg price yet

        if kraken_success and kraken_filled_qty is not None and kraken_filled_qty > 1e-9:
            hl_qty_target = kraken_filled_qty # Match Kraken fill exactly
            self.logger.info(f"Entry ({asset}): Placing HL SELL SHORT order for Qty={hl_qty_target:.8f} (matching Kraken fill)...")
            try:
                 # place_hl_order handles limits, retries, fill wait, verification
                 hl_success, hl_avg_price = self.place_hl_order(asset=asset, is_entry=True, position_size_qty=hl_qty_target)

                 if hl_success:
                      self.logger.info(f"Entry ({asset}): HL SELL SHORT order placed and verified filled.")
                 else:
                      self.logger.error(f"Entry ({asset}): place_hl_order (SELL SHORT) failed after Kraken BUY succeeded.")
                      hl_success = False

            except Exception as e:
                self.logger.exception(f"Entry ({asset}): Exception during HL SELL SHORT order placement: {e}")
                hl_success = False

            # --- Handle HL Failure after Kraken Success (CRITICAL REVERT) ---
            if not hl_success:
                self.logger.critical(f"Entry ({asset}): HL leg FAILED after Kraken leg succeeded! Attempting to EMERGENCY SELL Kraken position Qty={kraken_filled_qty:.8f}...")
                try:
                    # Attempt to sell back the exact Kraken quantity
                    revert_success, _ = self.place_kraken_order(asset=asset, is_entry=False, position_size=kraken_filled_qty)
                    if revert_success:
                        self.logger.info(f"Entry ({asset}): Successfully reverted Kraken position after HL failure.")
                    else:
                        self.logger.critical(f"Entry ({asset}): FAILED TO REVERT KRAKEN POSITION after HL failure. MANUAL INTERVENTION REQUIRED! Risk Exposure! Qty={kraken_filled_qty:.8f}")
                        # Trigger alerts! Bot state is inconsistent.
                except Exception as revert_e:
                 self.logger.critical(f"Entry ({asset}): HL leg FAILED after Kraken leg succeeded! Attempting to EMERGENCY SELL Kraken position Qty={kraken_filled_qty:.8f}...")
                 try:
                      # Attempt to sell back the exact Kraken quantity
                      revert_success, _ = self.place_kraken_order(asset=asset, is_entry=False, position_size=kraken_filled_qty)
                      if revert_success:
                           self.logger.info(f"Entry ({asset}): Successfully reverted Kraken position after HL failure.")
        else:
                           self.logger.critical(f"Entry ({asset}): FAILED TO REVERT KRAKEN POSITION after HL failure. MANUAL INTERVENTION REQUIRED! Risk Exposure! Qty={kraken_filled_qty:.8f}")
                           # Trigger alerts! Bot state is inconsistent.
                 except Exception as revert_e:
                      self.logger.critical(f"Entry ({asset}): EXCEPTION during Kraken revert attempt: {revert_e}. MANUAL INTERVENTION REQUIRED! Risk Exposure! Qty={kraken_filled_qty:.8f}")
                    return False  # Return failure as the entry was not successful

        elif not kraken_success:
             # Kraken order failed initially, no action needed for HL
             self.logger.info(f"Entry ({asset}): Skipping HL order because Kraken BUY failed.")
             return False
            else:  # Should not happen if kraken_success is True but kraken_filled_qty is bad/None
             self.logger.error(f"Entry ({asset}): Inconsistent state after Kraken BUY attempt. Success={kraken_success}, FilledQty={kraken_filled_qty}. Aborting entry.")
             return False

        # --- Final State Update on Full Success ---
        if kraken_success and hl_success:
            self.logger.info(f"✅ Entry ({asset}): Both legs successfully executed and verified.")
                with self.lock:  # Lock for updating shared state
                asset_data['in_position'] = True
                asset_data['entry_timestamp'] = time.time()
                # Store actual filled quantities
                    asset_data['position_size_hl_qty'] = hl_qty_target  # Should match kraken_filled_qty
                asset_data['position_size_kraken_qty'] = kraken_filled_qty
                # Calculate USD values based on Kraken fill price (HL avg price not reliably available)
                asset_data['position_size_hl_usd'] = hl_qty_target * kraken_avg_price if kraken_avg_price else None
                asset_data['position_size_kraken_usd'] = kraken_filled_qty * kraken_avg_price if kraken_avg_price else None
                asset_data['last_error'] = None # Clear last error on success
                self.active_positions.add(asset)
                self.entry_candidates.discard(asset) # Remove from candidates
            self.logger.info(f"Entry ({asset}): Updated internal state. HL Qty: {asset_data['position_size_hl_qty']:.8f}, Kraken Qty: {asset_data['position_size_kraken_qty']:.8f}")
            self.logger.info(f"Entry ({asset}): Stored USD sizes (estimated): HL=${asset_data['position_size_hl_usd']:.2f}, Kraken=${asset_data['position_size_kraken_usd']:.2f}")
            return True

            # Should technically not be reached if revert logic works, but as a safeguard:
            self.logger.error(f"❌ Entry ({asset}): Failed to complete position entry on both exchanges (reached end of function unexpectedly).")
            return False
    

    def exit_positions(self, asset: str) -> bool:
        """
        Orchestrates the exit from a position for a given asset, ensuring atomicity.
        Retrieves stored QTYs, closes HL SHORT (BUY), then Kraken LONG (SELL).
        Handles failures and logs critical errors for manual intervention.
        
        Args:
            asset: The asset symbol (e.g., 'BTC').
            
        Returns:
            True if both legs were successfully closed, False otherwise.
        """
        self.logger.info(f"--- Attempting Position Exit for {asset} ---")
        if asset not in self.assets:
            self.logger.error(f"Exit ({asset}): Asset configuration not found.")
            return False

            asset_data = self.assets[asset]
            
        # --- Check if actually in position ---
        if not asset_data.get('in_position', False):
            self.logger.warning(f"Exit ({asset}): Request ignored, not marked as in position.")
            # If not in position, should we ensure state is cleared? Or just return?
            # Returning False indicates no exit action was needed/taken successfully.
            return False

        # --- Retrieve Stored Quantities ---
        hl_qty_to_close = asset_data.get('position_size_hl_qty')
        kraken_qty_to_close = asset_data.get('position_size_kraken_qty')

        # Use a small tolerance for zero check
        if hl_qty_to_close is None or kraken_qty_to_close is None or hl_qty_to_close <= 1e-9 or kraken_qty_to_close <= 1e-9:
            self.logger.error(f"Exit ({asset}): Cannot exit, invalid or zero stored position quantity (HL: {hl_qty_to_close}, Kraken: {kraken_qty_to_close}). Manual intervention required.")
            # Do not clear state automatically here, needs investigation.
            # Trigger alert?
            return False

        self.logger.info(f"Exit ({asset}): Target Closing Quantities: HL BUY={hl_qty_to_close:.8f}, Kraken SELL={kraken_qty_to_close:.8f}")

        # --- Close HL SHORT Position (BUY) ---
        hl_closed_successfully = False
        try:
            # close_hl_position now uses the robust place_hl_order internally
            # It handles getting the precise size from API and placing the closing BUY order
            hl_closed_successfully = self.close_hl_position(asset) # Pass only asset name
            if hl_closed_successfully:
                 self.logger.info(f"Exit ({asset}): Hyperliquid BUY order (close short) reported success.")
                else:
                 # close_hl_position already logs errors extensively
                 self.logger.error(f"Exit ({asset}): close_hl_position reported failure. Halting exit process. MANUAL INTERVENTION REQUIRED for {asset}.")
                 # Do not proceed to Kraken close if HL failed. Potential risk.
                return False  # Exit failed
        except Exception as e:
            self.logger.exception(f"Exit ({asset}): Exception during close_hl_position call: {e}. Halting exit.")
            return False # Exit failed

        # --- Close Kraken LONG Position (SELL) - ONLY if HL close succeeded ---
        kraken_closed_successfully = False
        if hl_closed_successfully:
            self.logger.info(f"Exit ({asset}): Attempting Kraken SELL order for Qty={kraken_qty_to_close:.8f}...")
            try:
                # Use place_kraken_order with is_entry=False and the stored Kraken quantity
                kraken_closed_successfully, _ = self.place_kraken_order(asset=asset, is_entry=False, position_size=kraken_qty_to_close)
                if kraken_closed_successfully:
                    self.logger.info(f"Exit ({asset}): Kraken SELL order (close long) reported success.")
                else:
                     # place_kraken_order logs errors internally
                     self.logger.error(f"Exit ({asset}): place_kraken_order (SELL) reported failure after HL leg was closed.")
                     # CRITICAL STATE: HL closed, Kraken failed to close.
                     self.logger.critical(f"Exit ({asset}): HL leg closed, but Kraken SELL failed. POSITION MISMATCH! MANUAL INTERVENTION REQUIRED to sell {kraken_qty_to_close:.8f} {asset} on Kraken.")
                     # Do NOT clear the 'in_position' flag or quantities automatically.
                     asset_data['last_error'] = f"Exit failed: HL closed, Kraken SELL failed {datetime.now()}"
                     # Trigger alert!
                     return False  # Exit failed, state inconsistent
            except Exception as e:
                 self.logger.exception(f"Exit ({asset}): Exception during place_kraken_order (SELL) call: {e}. HL leg was already closed.")
                 self.logger.critical(f"Exit ({asset}): Exception during Kraken SELL after HL closed. POSITION MISMATCH! MANUAL INTERVENTION REQUIRED.")
                 asset_data['last_error'] = f"Exit exception: HL closed, Kraken SELL error {datetime.now()}"
                return False  # Exit failed, state inconsistent
                    else:
            # Should not be reached if HL close failed earlier, but as safeguard:
             self.logger.error(f"Exit ({asset}): Logic error - Reached Kraken close section despite HL close failure.")
             return False

        # --- Final State Update on Full Success ---
        if hl_closed_successfully and kraken_closed_successfully:
            self.logger.info(f"✅ Exit ({asset}): Both legs successfully closed.")
            with self.lock:  # Lock for updating shared state
                asset_data['in_position'] = False
                asset_data['entry_timestamp'] = None
                asset_data['position_size_hl_usd'] = None  # Clear estimated USD values
                asset_data['position_size_kraken_usd'] = None
                asset_data['position_size_hl_qty'] = None  # Clear exact QTYs
                asset_data['position_size_kraken_qty'] = None
                asset_data['last_error'] = None  # Clear error on success
                self.active_positions.discard(asset)
                # Keep asset in self.assets for future trades, just reset state.
            self.logger.info(f"Exit ({asset}): Cleared internal position state.")
            return True

            # Should not be reached if error handling above is correct
            self.logger.error(f"❌ Exit ({asset}): Failed to complete position exit cleanly (reached end of function unexpectedly). State may be inconsistent.")
            return False


    # --- Helper & API Methods --- (Keep existing implementations)

    # Keep the existing implementations of:
    # - get_kraken_signature
    # - kraken_request
    # - get_kraken_ticker
    # - update_kraken_prices (if still needed besides WS)
    # - get_hl_order_book (if needed for limit orders)
    # - check_book_depth (if needed for limit orders)
    # - place_hl_order (ensure it takes qty, returns success/price)
    # - verify_hl_position (if needed)
    # - place_kraken_order (ensure it takes qty, returns success/price)
    # - wait_for_kraken_fill (if place_kraken_order is async)
    # - get_kraken_order_fill_info (if place_kraken_order is async)
    # - wait_for_hl_fill (if place_hl_order is async)
    # - cancel_hl_order
    # - cancel_kraken_order
    # - close_hl_position (ensure it correctly closes the current short)
    # - get_hl_position_size
    # - synchronize_positions
    # - check_balances (ensure it returns structure used by _calculate_position_sizes)
    # - calculate_historical_percentile (Adapt to take needed percentiles set)
    # - check_for_flash_crash
    # - check_margin_levels
    # - emergency_close_all_positions
    # - enforce_kraken_rate_limit
    # - get_recent_volatility
    # - execute_with_cooldown
    # - is_flash_crash_cooldown_active

    # --- Placeholder / Modified Helper Methods ---

    def check_balances(self) -> Dict:
        """
        Checks balances on Kraken and Hyperliquid.
        Returns a dictionary with structure needed by _calculate_position_sizes.
        (This is the implementation from the original script - ensure it's correct)
        """
        self.logger.debug("Checking balances...")
        kraken_balance = 0.0
        kraken_success = False
        kraken_error = None
        hl_available_margin = 0.0
        hl_total_margin = 0.0 # May not be directly available/relevant for available margin check
        hl_success = False
        hl_error = None

        # Check Kraken balance
        try:
            # Use existing kraken_request implementation
            kraken_data = {"nonce": str(int(1000*time.time()))}
            result = self.kraken_request('/0/private/Balance', kraken_data)

            if result and 'result' in result and not result.get('error'):
                # Assuming USD balance is ZUSD or USD
                kraken_balance = float(result['result'].get('ZUSD', result['result'].get('USD', 0.0)))
                kraken_success = True
                self.logger.debug(f"Kraken balance check successful: ${kraken_balance:.2f}")
            else:
                kraken_error = result.get('error', ['Unknown Kraken balance error'])[0]
                self.logger.warning(f"Kraken balance check failed: {kraken_error}")
        except Exception as e:
            kraken_error = f"Exception checking Kraken balance: {str(e)}"
            self.logger.exception(f"Exception checking Kraken balance: {e}")

        # Check Hyperliquid balance/margin
        try:
            if self.hl_info and self.hl_wallet:
                # Use user_state to get margin summary
                user_state = self.hl_info.user_state(self.hl_wallet)
                if user_state and 'marginSummary' in user_state:
                    margin_summary = user_state['marginSummary']
                    # 'accountValue' might represent total equity including PnL
                    # 'availableMargin' might be more relevant for new positions
                    # Using accountValue as per example, verify if availableMargin is better
                    hl_available_margin = float(margin_summary.get('accountValue', 0.0))
                    hl_success = True
                    self.logger.debug(f"Hyperliquid balance check successful: Available Margin/Account Value ${hl_available_margin:.2f}")
                else:
                    hl_error = "Failed to retrieve marginSummary from user_state"
                    self.logger.warning(f"Hyperliquid balance check failed: {hl_error}")
            else:
                 hl_error = "Hyperliquid Info client or wallet address not initialized"
                 self.logger.warning(f"Hyperliquid balance check failed: {hl_error}")
        except Exception as e:
            hl_error = f"Exception checking Hyperliquid balance: {str(e)}"
            self.logger.exception(f"Exception checking Hyperliquid balance: {e}")


        # Consolidate results
        balances_result = {
            'kraken': {'balance': kraken_balance, 'success': kraken_success, 'error': kraken_error},
            'hyperliquid': {'available': hl_available_margin, 'total': hl_available_margin, 'success': hl_success, 'error': hl_error} # Using available for both total/avail for simplicity
        }

        # Determine overall sufficiency and error status
        has_error = not (kraken_success and hl_success)
        # Sufficient if both balances are above safety buffer + minimums
        is_sufficient = (kraken_success and hl_success and
                         kraken_balance >= self.EXCHANGE_SAFETY_BUFFER_USD and
                         hl_available_margin >= (self.HL_MIN_ORDER_USD + self.EXCHANGE_SAFETY_BUFFER_USD))

        balances_result['sufficient'] = is_sufficient
        balances_result['error'] = has_error
        balances_result['details'] = f"Kraken Err: {kraken_error}, HL Err: {hl_error}" if has_error else None

        if has_error:
            self.logger.warning(f"Balance check completed with errors. Sufficient: {is_sufficient}. Details: {balances_result['details']}")
        elif not is_sufficient:
            self.logger.warning(f"Balance check completed. Balances may be insufficient. Sufficient: {is_sufficient}")
        else:
            self.logger.debug("Balance check successful and sufficient.")

        return balances_result


    def calculate_historical_percentile(self, asset: str, percentiles_needed: Set[int]) -> Optional[Dict[str, float]]:
        """
        Calculates historical funding rate percentiles using Hyperliquid API.
        Modified to calculate only the specifically needed percentiles.
        
        Args:
            asset: The asset symbol (e.g., 'BTC').
            percentiles_needed: A set of integer percentiles required (e.g., {60, 50}).
            
        Returns:
            A dictionary mapping percentile string to rate value (e.g., {'60': 0.015, '50': 0.005}),
            or None if calculation fails.
        """
        if not percentiles_needed:
            self.logger.info(f"No percentiles needed for {asset}, skipping calculation.")
            return {} # Return empty dict if none needed

        self.logger.info(f"Calculating historical percentiles {list(percentiles_needed)} for {asset}...")
        try:
            # Start from current time and get enough history for 500 samples
            current_date = datetime.now()
            end_time = int(current_date.timestamp() * 1000)
            # Go back 20 days to ensure we get enough samples
            start_time = end_time - (20 * 24 * 3600 * 1000)

            # Get funding history using the Info client
            funding_history = self.hl_info.funding_history(asset, start_time, end_time)

            if not funding_history:
                self.logger.warning(f"No funding history returned for {asset} in the last 20 days.")
                return None

            # Sort by timestamp in descending order (most recent first)
            sorted_history = sorted(funding_history, key=lambda x: int(x['time']), reverse=True)
            
            # Take exactly 500 samples if available, otherwise take all samples
            sample_size = min(500, len(sorted_history))
            sorted_history = sorted_history[:sample_size]
            
            # Convert funding rates to percentage
            rates = [float(entry['fundingRate']) * 100 for entry in sorted_history]

            if not rates:
                self.logger.warning(f"Could not extract valid funding rates from history for {asset}.")
            return None
    
            self.logger.info(f"Retrieved {len(rates)} historical funding rate data points for {asset}.")
            self.logger.info(f"Sample size: {sample_size}")
            self.logger.info(f"Mean rate: {np.mean(rates):.4f}%")
            self.logger.info(f"Max rate: {np.max(rates):.4f}%")

            # Calculate percentiles
            calculated_percentiles = {}
            for p in percentiles_needed:
                calculated_percentiles[str(p)] = np.percentile(rates, p)

            # Log the results
            log_str = f"Calculated {asset} percentiles: " + ", ".join([f"{p}%: {v:.4f}" for p, v in calculated_percentiles.items()])
            self.logger.info(log_str)

            # Print most recent rates for debugging
            self.logger.info("\nMost recent funding rates:")
            for entry in sorted_history[:5]:
                rate = float(entry['fundingRate']) * 100
                timestamp = datetime.fromtimestamp(int(entry['time'])/1000)
                self.logger.info(f"{timestamp}: {rate:.4f}%")

            return calculated_percentiles

        except Exception as e:
            self.logger.exception(f"Error calculating historical percentiles for {asset}: {e}")
            return None

    # --- Main Loop ---

    def run(self):
        """Main bot execution loop. Primarily driven by WebSocket events and periodic checks."""
        self.logger.info(f"Starting {type(self).__name__} run loop...")

        if not self.running:
            self.logger.warning("Bot is not marked as running. Exiting run loop immediately.")
            return

        # Ensure initialization is complete (essential for WS connection etc.)
        if not self.hl_exchange or not self.hl_info or not self.assets:
             self.logger.error("Bot not properly initialized (APIs/Assets missing). Cannot run.")
             self.running = False
             return

        # Connect WebSocket if not already running (initialization should handle this)
        if not self.ws_thread or not self.ws_thread.is_alive():
             self.logger.warning("WebSocket not running at start of run loop. Attempting to connect...")
             self.connect_websocket()
             time.sleep(5) # Allow time for connection attempt

        try:
            last_periodic_check = 0
            check_interval = 30 # Seconds for periodic checks (balances, sync)

            while self.running:
                current_time = time.time()

                # --- Periodic Checks ---
                if current_time - last_periodic_check >= check_interval:
                    self.logger.debug(f"Performing periodic checks (Interval: {check_interval}s)...")
                    last_periodic_check = current_time

                    # Check balances (can identify issues even if WS is running)
                    self.execute_with_cooldown(self.check_balances)

                    # Check margin levels for safety
                    self.execute_with_cooldown(self.check_margin_levels)

                    # Synchronize positions (optional, if state drift is suspected)
                    # for asset in list(self.active_positions):
                    #     self.execute_with_cooldown(self.synchronize_positions, asset)

                    # Check WebSocket connection status (can trigger reconnect if manager failed)
                    if not self.ws or not self.ws.sock or not self.ws.sock.connected:
                         self.logger.warning("Detected potentially disconnected WebSocket. Manager should handle reconnect.")
                         # Optional: Force a reconnect attempt if manager seems stuck
                         # if not self.ws_thread or not self.ws_thread.is_alive():
                         #     self.logger.error("WebSocket manager thread seems dead! Attempting restart.")
                         #     self.stop_websocket()
                         #     self.connect_websocket()


                # --- Core logic is event-driven by WebSocket messages ---
                # process_websocket_messages runs in a separate thread
                # check_entry/exit_conditions are called from message processing

                # --- Log Summary Periodically ---
                # (Add logging here if needed - e.g., current rates, position status)


                # Sleep briefly to prevent high CPU usage in the main loop
                                time.sleep(1)

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received. Initiating shutdown...")
            self.shutdown()
        except Exception as e:
            self.logger.exception(f"Critical error in main run loop: {e}")
            self.logger.error("Attempting emergency shutdown due to critical error.")
            self.shutdown(emergency=True) # Pass emergency flag if defined
        finally:
            self.logger.info(f"{type(self).__name__} run loop finished.")
            # Ensure cleanup happens even if shutdown wasn't called (e.g., error before shutdown)
            if self.running: # If error occurred without graceful shutdown
                 self.shutdown(emergency=True)


    def shutdown(self, emergency=False):
        """Gracefully shut down the bot, closing WebSocket and potentially positions."""
        if not self.running and not emergency: # Prevent multiple shutdown calls unless emergency
             self.logger.warning("Shutdown already in progress or completed.")
             return

        self.logger.info(f"--- Initiating Bot Shutdown (Emergency: {emergency}) ---")
        self.running = False # Signal all loops to stop

        # Stop WebSocket connection
        self.stop_websocket()

        # --- Optional: Close open positions on shutdown ---
        # Consider making this configurable (e.g., via config file or shutdown signal)
        close_on_shutdown = False # Set to True or load from config if desired
        if close_on_shutdown or emergency:
            self.logger.warning("Attempting to close all active positions on shutdown...")
            active_now = list(self.active_positions) # Copy set for iteration
            if not active_now:
                 self.logger.info("No active positions to close.")
                            else:
                 for asset in active_now:
                      self.logger.info(f"Closing position for {asset} due to shutdown...")
                      # Use the standard exit mechanism
                    if not self.exit_positions(asset):
                           # Consider emergency close attempt if standard fails
                        self.emergency_close_all_positions()  # This might be too aggressive
                        else:
             self.logger.info("Shutdown requested without closing active positions.")

        # Wait for message processor thread to finish
        # (Queue processing loop should exit when self.running is False)
        # No explicit join needed if it's a daemon thread, but cleaner to manage

        self.logger.info("--- Bot Shutdown Complete ---")


    # --- KEEP ALL OTHER ORIGINAL HELPER METHODS FROM ArbBotBase ---
    # Ensure implementations for methods like:
    # get_kraken_signature, kraken_request, get_kraken_ticker, update_kraken_prices,
    # get_hl_order_book, check_book_depth, place_hl_order, verify_hl_position,
    # place_kraken_order, wait_for_kraken_fill, get_kraken_order_fill_info,
    # wait_for_hl_fill, cancel_hl_order, cancel_kraken_order, close_hl_position,
    # get_hl_position_size, synchronize_positions, check_for_flash_crash,
    # check_margin_levels, emergency_close_all_positions, enforce_kraken_rate_limit,
    # get_recent_volatility, execute_with_cooldown, is_flash_crash_cooldown_active
    # ARE PRESENT AND CORRECT below this point.
    # ========================================================================
    # PASTE ORIGINAL HELPER METHOD IMPLEMENTATIONS HERE
    # ========================================================================

    # Example placeholder for a required method (replace with actual implementation)
    def get_kraken_signature(self, urlpath: str, data: dict) -> str:
        """(Placeholder - Requires Actual Implementation)"""
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.kraken_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    def kraken_request(self, uri_path: str, data: dict) -> dict:
        """(Placeholder - Requires Actual Implementation)"""
        if not self.kraken_key or not self.kraken_secret:
            return {'error': ['Kraken API credentials not set.']}

        self.enforce_kraken_rate_limit() # Use rate limit helper

        headers = {'API-Key': self.kraken_key, 'API-Sign': self.get_kraken_signature(uri_path, data)}

        try:
            response = requests.post((self.kraken_api_url + uri_path), headers=headers, data=data, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            self.kraken_api_last_call = time.time()
            self.kraken_call_times.append(self.kraken_api_last_call) # Track call time
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Kraken API request failed: {e}")
            return {'error': [f'Kraken API request exception: {str(e)}']}
        except json.JSONDecodeError as e:
             self.logger.error(f"Failed to decode Kraken JSON response: {e}")
             return {'error': [f'Kraken JSON decode error: {str(e)}']}


    def enforce_kraken_rate_limit(self):
        """(Placeholder - Requires Actual Implementation)"""
        now = time.time()
        # Sleep if last call was too recent
        time_since_last_call = now - self.kraken_api_last_call
        if time_since_last_call < self.kraken_api_min_interval:
            sleep_time = self.kraken_api_min_interval - time_since_last_call
            self.logger.debug(f"Enforcing Kraken minimum interval. Sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)
            now = time.time() # Update time after sleep

        # Prune old call times outside the window
        while self.kraken_call_times and (now - self.kraken_call_times[0] > self.kraken_rate_limit_window):
            self.kraken_call_times.popleft()

        # Check call count within the window
        if len(self.kraken_call_times) >= self.kraken_max_calls_per_window:
            # Calculate wait time until the oldest call expires
            wait_time = (self.kraken_call_times[0] + self.kraken_rate_limit_window) - now + 0.1 # Add small buffer
            self.logger.warning(f"Approaching Kraken rate limit ({len(self.kraken_call_times)} calls). Waiting for {wait_time:.3f}s")
            if wait_time > 0:
                 time.sleep(wait_time)


    def get_kraken_ticker(self, asset: str) -> Tuple[Optional[float], Optional[float]]:
        """(Placeholder - Requires Actual Implementation)"""
        if asset not in self.assets or 'spec' not in self.assets[asset] or not self.assets[asset]['spec'].get('kraken_pair'):
            self.logger.error(f"Cannot get Kraken ticker for {asset}: Missing config or Kraken pair.")
            return None, None
        kraken_pair = self.assets[asset]['spec']['kraken_pair']
        uri_path = '/0/public/Ticker'
        data = {'pair': kraken_pair}
        try:
            # Public endpoint, no auth needed, but still rate limited potentially
            self.enforce_kraken_rate_limit() # Apply rate limit even for public endpoint
            response = requests.get(self.kraken_api_url + uri_path, params=data, timeout=5)
            response.raise_for_status()
            result = response.json()

            if result.get('error'):
                self.logger.error(f"Error fetching Kraken ticker for {kraken_pair}: {result['error']}")
                return None, None

            if 'result' in result and kraken_pair in result['result']:
                ticker_data = result['result'][kraken_pair]
                # b = best bid, a = best ask
                bid = float(ticker_data['b'][0])
                ask = float(ticker_data['a'][0])
                self.logger.debug(f"Kraken ticker for {asset} ({kraken_pair}): Bid={bid}, Ask={ask}")
                # Store latest prices
                self.assets[asset]['kraken_best_bid'] = bid
                self.assets[asset]['kraken_best_ask'] = ask
                return bid, ask
            else:
                self.logger.error(f"Kraken ticker response missing result or pair data for {kraken_pair}.")
                return None, None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Exception fetching Kraken ticker for {asset}: {e}")
            return None, None
        except Exception as e:
             self.logger.exception(f"Unexpected error fetching Kraken ticker for {asset}: {e}")
             return None, None


    def place_hl_order(self, asset: str, is_entry: bool, position_size_qty: float) -> Tuple[bool, Optional[float]]:
        """Place order on Hyperliquid using Limit Orders with depth check, retries, and fill verification.
        
        Receives the exact quantity (QTY) to trade, calculated externally based on dynamic sizing.
        
        Args:
            asset: The asset symbol (e.g., 'BTC').
            is_entry: True for entry (sell short), False for exit (buy to close).
            position_size_qty: The exact quantity (QTY) of the asset to trade.
            
        Returns:
            Tuple of (success_boolean, filled_avg_price_or_None) - Price may be None if not easily available from status.
        """
        side = "Sell" if is_entry else "Buy"
        is_buy_flag = not is_entry # SDK uses is_buy flag
        self.logger.info(f"Placing HL {side.upper()} Order: Asset={asset}, Qty={position_size_qty:.8f}")

        # --- Pre-checks ---
        if not self.hl_exchange:
            self.logger.error(f"Place HL Order ({asset}): HL Exchange client not initialized.")
            return False, None
        if asset not in self.assets or 'spec' not in self.assets[asset]:
            self.logger.error(f"Place HL Order ({asset}): Asset configuration or specs missing.")
            return False, None
        if position_size_qty <= 1e-9: # Use tolerance for zero check
            self.logger.error(f"Place HL Order ({asset}): Invalid or zero order size: {position_size_qty}")
            return False, None

        px_precision = self.assets[asset]['spec'].get('price_precision', 4)
        self.assets[asset]['current_hl_order_id'] = None # Reset any previous ID for this asset

        # --- Retry Loop ---
        for attempt in range(self.max_retries):
            self.logger.info(f"Place HL Order ({asset}): Attempt {attempt + 1}/{self.max_retries}...")
            try:
                # --- Get Prices & Check Depth ---
                # Fetch fresh book data for price calculation and depth check
                book_data = self.get_hl_order_book(asset)
                current_bid = self.assets[asset].get('hl_best_bid')
                current_ask = self.assets[asset].get('hl_best_ask')
                
                if current_bid is None or current_ask is None:
                    self.logger.error(f"Place HL Order ({asset}) Attempt {attempt+1}: Cannot get valid HL prices from order book.")
                    if attempt < self.max_retries - 1:
                        time.sleep(2)
                        continue
                    return False, None  # Failed after retries

                # Check depth *before* placing the order
                has_liquidity, avg_depth_price = self.check_book_depth(asset, is_buy_flag, position_size_qty)
                if not has_liquidity:
                    self.logger.error(f"Place HL Order ({asset}) Attempt {attempt+1}: Insufficient order book depth for size {position_size_qty:.8f}. Required depth factor: {getattr(self, 'min_order_book_depth', 1.5)}")
                    time.sleep(5)  # Wait longer if depth is the issue
                    if attempt < self.max_retries - 1:
                        continue
                    return False, None  # Failed after retries due to depth

                # --- Calculate Limit Price ---                
                mid_price = (current_bid + current_ask) / 2.0
                current_slippage = self.slippage_buffer * (attempt + 1)
                if is_entry:  # Selling (short), place below mid with slippage
                    limit_price = mid_price * (1 - current_slippage)
                else:  # Buying (close short), place above mid with slippage
                    limit_price = mid_price * (1 + current_slippage)
                limit_price_str = f"{limit_price:.{px_precision}f}"
                self.logger.info(f"Place HL Order ({asset}) Attempt {attempt+1}: Calculated Limit Price={limit_price_str} (Mid={mid_price:.{px_precision}f}, Slippage={(current_slippage*100):.2f}%)")

                # --- Place Order via SDK ---                
                order_result = self.hl_exchange.order(
                    coin=asset,
                    is_buy=is_buy_flag,
                    sz=position_size_qty,
                    limit_px=limit_price_str,
                    order_type={"limit": {"tif": "Gtc"}}  # Use GTC limit order
                )
                self.logger.debug(f"HL Order Result ({asset}, Attempt {attempt+1}): {order_result}")

                # --- Process Order Result ---                
                # ** CRITICAL: Verify actual success response structure **
                if order_result and order_result.get("status") == "ok":
                    response_data = order_result.get("response", {}).get("data", {})
                    statuses = response_data.get("statuses", [])
                    
                    order_id = None
                    if statuses:
                        status_info = statuses[0]  # Assuming first status is primary
                        if isinstance(status_info, dict):
                            if "resting" in status_info:
                                order_id = status_info["resting"].get("oid")
                            elif "filled" in status_info:
                                order_id = status_info["filled"].get("oid")
                        elif isinstance(status_info, str):  # Handle simple status strings like "success"
                            # OID might be elsewhere in response_data? Need actual examples.
                            pass
                    if not order_id:
                         # Fallback: Try to find OID in a potentially different location
                         # This needs confirmation based on real API responses
                         self.logger.warning(f"Place HL Order ({asset}): Could not extract Order ID from statuses: {statuses}. Check response structure.")
                         # Attempt to find oid if nested differently (example)
                         # if response_data.get('type') == 'order' and 'order' in response_data:
                         #    order_id = response_data['order'].get('oid') 
                         if not order_id: 
                              self.logger.error(f"Place HL Order ({asset}): FAILED to extract Order ID. Cannot track order. Response: {order_result}")
                              # Treat as failure if we cannot track the order
                              return False, None 
                    
                    self.assets[asset]['current_hl_order_id'] = order_id
                    self.logger.info(f"HL order {order_id} placed successfully for {asset}. Waiting for fill...")

                    # --- Wait for Fill ---                    
                    fill_status, filled_size_qty = self.wait_for_hl_fill(asset, order_id)
                    avg_fill_price = None  # Placeholder, wait_for_hl_fill doesn't return price yet
                    
                    if fill_status and abs(filled_size_qty - position_size_qty) < 1e-9:  # Check for full fill
                        self.logger.info(f"✅ HL order {order_id} ({asset}) confirmed FULLY filled ({filled_size_qty:.8f} Qty).")
                        # --- Verify Position --- 
                        if self.verify_hl_position(asset, filled_size_qty, is_entry):
                            self.assets[asset]['current_hl_order_id'] = None  # Clear OID on success
                            return True, avg_fill_price  # Success!
                        else:
                             self.logger.error(f"Place HL Order ({asset}): Position verification FAILED after fill! Manual check needed.")
                             self.assets[asset]['current_hl_order_id'] = None
                            return False, None  # Treat as failure
                    elif fill_status:  # Partially filled before timeout
                        self.logger.warning(f"HL order {order_id} ({asset}) only PARTIALLY filled ({filled_size_qty:.8f} / {position_size_qty:.8f}) within timeout. Attempting to cancel...")
                        self.cancel_hl_order(asset, order_id)
                        self.assets[asset]['current_hl_order_id'] = None
                        # Treat partial fill as failure for this strategy? Or try to adjust other leg?
                        # For now, treating as failure and letting retry loop handle it.
                        if attempt < self.max_retries - 1:
                            continue
                        else:
                            return False, None  # Failed after retries
                    else:  # fill_status is False (timeout with potentially zero or partial fill)
                        self.logger.warning(f"HL order {order_id} ({asset}) did not fill within timeout (Last Filled: {filled_size_qty:.8f}). Attempting to cancel...")
                        self.cancel_hl_order(asset, order_id)  # Attempt cancellation regardless
                        self.assets[asset]['current_hl_order_id'] = None
                        if attempt < self.max_retries - 1:
                            continue
                        else:
                            return False, None  # Failed after retries
                else:
                    # API call itself failed
                    error_details = order_result.get("response", "No response field")
                    self.logger.error(f"HL Order placement API call failed for {asset} (Attempt {attempt+1}). Status: {order_result.get('status')}, Error: {error_details}")
                    if attempt < self.max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        return False, None  # Failed after retries
        except Exception as e:
                self.logger.exception(f"Unexpected error during HL order placement for {asset} (Attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    return False, None  # Failed after retries

        # Should only be reached if all retries fail
        self.logger.error(f"❌ HL order placement for {asset} failed definitively after {self.max_retries} attempts.")
        return False, None
       
    def get_hl_order_book(self, asset: str, depth: int = 20) -> Optional[Dict[str, List[Tuple[float, float]]]]:
        """Fetches the L2 order book for an asset using the HL SDK's Info class."""
        # Note: BTCArbBot used requests for this, but using SDK Info is cleaner.
        self.logger.debug(f"Fetching HL L2 order book for {asset} (Depth: {depth})...")
        if not self.hl_info:
            self.logger.error(f"Get HL Order Book ({asset}): Hyperliquid Info client not initialized.")
            return None
        try:
            l2_book_data = self.hl_info.l2_book(coin=asset, num_levels=depth)
            # Expected structure: {'levels': [[bid_levels], [ask_levels]], 'time': timestamp}
            # where bid/ask_levels are lists of [price_str, size_str]

            if not l2_book_data or 'levels' not in l2_book_data or len(l2_book_data['levels']) != 2:
                self.logger.warning(f"Get HL Order Book ({asset}): Invalid data structure received: {l2_book_data}")
                return None

            bids = []
            asks = []
            # Safely parse bids
            for level in l2_book_data['levels'][0]:  # bids are first element
                try:
                    bids.append((float(level[0]), float(level[1])))
                except (ValueError, TypeError, IndexError):
                    self.logger.warning(f"Get HL Order Book ({asset}): Invalid bid level format: {level}")
                    continue
            # Safely parse asks
            for level in l2_book_data['levels'][1]:  # asks are second element
                try:
                    asks.append((float(level[0]), float(level[1])))
                except (ValueError, TypeError, IndexError):
                    self.logger.warning(f"Get HL Order Book ({asset}): Invalid ask level format: {level}")
                    continue

            # Update best bid/ask in asset state (use lock for thread safety)
            with self.lock:
                if asset in self.assets:
                    if bids:
                        self.assets[asset]['hl_best_bid'] = bids[0][0]
                    if asks:
                        self.assets[asset]['hl_best_ask'] = asks[0][0]

            return {'bids': bids, 'asks': asks}
            except Exception as e:
            self.logger.exception(f"Error getting HL order book for {asset}: {e}")
            return None
        
    def check_book_depth(self, asset: str, is_buy: bool, size_qty: float) -> Tuple[bool, float]:
        """Checks if there's enough liquidity in the HL order book for a given size."""
        self.logger.debug(f"Checking HL book depth for {asset}: Side={'Buy' if is_buy else 'Sell'}, Size={size_qty:.8f}")
        try:
            book = self.get_hl_order_book(asset, depth=20)  # Fetch reasonable depth
            if not book:
                self.logger.warning(f"Check HL Depth ({asset}): Failed to get order book.")
        return False, 0.0
    
            levels = book['asks'] if is_buy else book['bids']
            # Use min_order_book_depth defined in __init__ (ensure it's set, e.g., 1.5)
            min_depth_multiplier = getattr(self, 'min_order_book_depth', 1.5)
            required_size_with_buffer = size_qty * min_depth_multiplier

            cumulative_size = 0.0
            weighted_price_sum = 0.0

            for price, level_size in levels:
                size_from_this_level = min(level_size, required_size_with_buffer - cumulative_size)
                if size_from_this_level <= 1e-9:
                    continue  # Skip negligible amounts

                weighted_price_sum += price * size_from_this_level
                cumulative_size += size_from_this_level

                if cumulative_size >= required_size_with_buffer:
                    break

            if cumulative_size < required_size_with_buffer:
                self.logger.warning(f"Check HL Depth ({asset}): Insufficient liquidity. Required={required_size_with_buffer:.8f}, Available={cumulative_size:.8f}")
                return False, 0.0

            if cumulative_size <= 1e-9:
                return False, 0.0  # Avoid division by zero

            avg_price = weighted_price_sum / cumulative_size
            px_precision = self.assets[asset]['spec'].get('price_precision', 4)
            self.logger.debug(f"Check HL Depth ({asset}): Sufficient liquidity. Avg Price for {cumulative_size:.8f} Qty = {avg_price:.{px_precision}f}")
            return True, avg_price
        except Exception as e:
            self.logger.exception(f"Error checking HL order book depth for {asset}: {e}")
            return False, 0.0
        
    def wait_for_hl_fill(self, asset: str, order_id: int) -> Tuple[bool, float]:
        """Wait for Hyperliquid order to fill using get_order_status."""
        # Adapted from BTCArbBot, parameterized for asset/oid
        self.logger.info(f"Waiting for HL fill for order {order_id} ({asset})...")
        start_time = time.time()
        last_known_filled_size = 0.0
        last_logged_fill_size = -1.0 # Track last logged size to reduce log spam
        
        while time.time() - start_time < self.order_timeout:
            try:
                if not self.hl_exchange:
                    self.logger.error(f"Wait HL Fill ({order_id}): HL Exchange client not initialized.")
                    return False, last_known_filled_size

                # ** CRITICAL: Verify response structure with actual SDK usage or docs **
                # Assuming hl_exchange.order_status(asset, order_id) exists and returns dict like:
                # {"order": {..., "status": "...", "filledSize": "...", "totalSize": "..."}, ...}
                order_status_response = self.hl_exchange.order_status(asset, order_id)

                if not order_status_response or 'order' not in order_status_response:
                    self.logger.warning(f"Wait HL Fill ({order_id}): Invalid response from order_status: {order_status_response}")
                    time.sleep(2)
                    continue

                order_info = order_status_response['order']
                status = order_info.get('status')
                current_filled_size_str = order_info.get('filledSize')
                total_size_str = order_info.get('totalSize')

                try:
                     current_filled_size = float(current_filled_size_str) if current_filled_size_str is not None else 0.0
                     total_size = float(total_size_str) if total_size_str is not None else 1.0
                     last_known_filled_size = current_filled_size # Update last known fill
                except (ValueError, TypeError):
                     self.logger.error(f"Wait HL Fill ({order_id}): Could not parse sizes: filled='{current_filled_size_str}', total='{total_size_str}'")
                     time.sleep(2)
                    continue
                    
                # Log partial fills if size increased, but not too frequently
                if status == 'open' and current_filled_size > last_logged_fill_size:
                     fill_percent = (current_filled_size / total_size) * 100 if total_size > 0 else 0
                     self.logger.info(f"HL order {order_id} {fill_percent:.1f}% filled ({current_filled_size:.8f}/{total_size:.8f} {asset})")
                     last_logged_fill_size = current_filled_size

                if status == 'filled':
                    self.logger.info(f"HL order {order_id} filled. Total Filled Size: {current_filled_size:.8f} {asset}")
                    return True, current_filled_size
                elif status not in ['open', 'posted']:
                     self.logger.warning(f"HL order {order_id} has terminal status '{status}' (not filled). Fill failed.")
                     return False, current_filled_size # Return current filled amount on failure

                time.sleep(1)
            except Exception as e:
                self.logger.exception(f"Error checking HL order status {order_id}: {e}")
                time.sleep(2)

        self.logger.error(f"Timed out waiting for HL order {order_id} ({asset}) after {self.order_timeout} seconds.")

        # Final check after timeout
        try:
            order_status_response = self.hl_exchange.order_status(asset, order_id)
            if order_status_response and order_status_response.get('order', {}).get('status') == 'filled':
                filled_size_str = order_status_response['order'].get('filledSize')
                filled_size = float(filled_size_str) if filled_size_str is not None else 0.0
                self.logger.info(f"HL order {order_id} filled just after timeout. Filled Size: {filled_size:.8f}")
                return True, filled_size
            else:
                # Update last known fill size one last time on timeout
                 if order_status_response and 'order' in order_status_response:
                     filled_size_str = order_status_response['order'].get('filledSize')
                     last_known_filled_size = float(filled_size_str) if filled_size_str is not None else last_known_filled_size
        except Exception as e:
            self.logger.warning(f"Error during final check for HL order {order_id}: {e}")

        return False, last_known_filled_size # Return last known filled size on timeout
    
    def verify_hl_position(self, asset: str, expected_size_qty: float, is_entry: bool) -> bool:
        """Verify Hyperliquid position matches the expected size after an order."""
        # expected_size_qty should be the absolute value intended
        target_size_signed = -abs(expected_size_qty) if is_entry else 0.0
        self.logger.debug(f"Verifying HL position for {asset}. Expected Size Qty = {target_size_signed:.8f}")

        try:
            # Allow some time for position update after fill
            time.sleep(2)
            actual_size_qty = self.get_hl_position_size(asset)

            if actual_size_qty is None:
                self.logger.error(f"Verify HL Position ({asset}): Failed to get current position size for verification.")
                return False
                
            # Define a small tolerance for comparison
            tolerance = 0.000001 # Adjust if needed
            
            if abs(actual_size_qty - target_size_signed) < tolerance:
                self.logger.info(f"✅ Verify HL Position ({asset}): Actual size {actual_size_qty:.8f} matches target {target_size_signed:.8f}.")
                return True
            else:
                self.logger.error(f"❌ Verify HL Position ({asset}): Mismatch! Target={target_size_signed:.8f}, Actual={actual_size_qty:.8f}")
                return False
                
        except Exception as e:
            self.logger.exception(f"Error verifying HL position for {asset}: {e}")
            return False
    
    def cancel_hl_order(self, asset: str, order_id: int) -> bool:
        """Cancel a specific Hyperliquid order by its OID using SDK Exchange."""
        if not order_id:
            self.logger.warning(f"Cancel HL Order ({asset}): No order_id provided.")
            return False
        self.logger.info(f"Attempting to cancel HL order {order_id} for asset {asset}...")
        if not self.hl_exchange:
            self.logger.error(f"Cancel HL Order ({order_id}): HL Exchange client not initialized.")
            return False
        try:
            # Assuming SDK method cancel(asset, oid) exists
            cancel_response = self.hl_exchange.cancel(asset, order_id)
            self.logger.debug(f"HL Cancel Response ({order_id}): {cancel_response}")

            # ** CRITICAL: Verify actual success response structure from SDK **
            # Assuming format: {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success" or error msg]}}}
            if cancel_response and cancel_response.get("status") == "ok":
                response_data = cancel_response.get("response", {}).get("data", {})
                statuses = response_data.get("statuses", [])
                if statuses and statuses[0] == "success":
                    self.logger.info(f"Successfully cancelled HL order {order_id} for {asset}.")
                    return True
                else:
                     error_msg = statuses[0] if statuses else "Unknown cancel status"
                     self.logger.error(f"HL cancel request for {order_id} failed status: {error_msg}")
                     # Check if order was already gone
                     if "Order already inactive" in str(error_msg) or "Order not found" in str(error_msg):
                          self.logger.warning(f"HL order {order_id} already inactive/not found, considering cancellation successful.")
                          return True
                     return False
            else:
                 error_details = cancel_response.get("response", "No response field")
                 self.logger.error(f"HL Cancel API call failed for {order_id}. Status: {cancel_response.get('status')}, Error: {error_details}")
                 return False
        except Exception as e:
            self.logger.exception(f"Error cancelling HL order {order_id}: {e}")
            return False

    def place_kraken_order(self, asset: str, is_entry: bool, position_size: Optional[float] = None) -> Tuple[bool, Optional[float]]:
        """
        Places an order on Kraken. Assumes QTY is passed in position_size.
        (Placeholder - Requires Actual Implementation using kraken_request)
        Returns: (success_boolean, filled_price_or_None)
        """
        self.logger.info(f"Placing Kraken Order: Asset={asset}, Entry={is_entry}, Qty={position_size:.8f}")
        if asset not in self.assets or 'spec' not in self.assets[asset]: return False, None
        kraken_pair = self.assets[asset]['spec'].get('kraken_pair')
        if not kraken_pair: return False, None

        if position_size is None or position_size <= 0:
             self.logger.error(f"Kraken Order {asset}: Invalid position size {position_size}.")
             return False, None

        order_type = 'buy' if is_entry else 'sell'
        trade_type = 'market'  # Using market orders for simplicity in placeholder
            
            data = {
                "nonce": str(int(1000*time.time())),
            "ordertype": trade_type,
            "type": order_type,
            "volume": f"{position_size:.8f}", # Volume in base currency (QTY)
            "pair": kraken_pair,
            # "validate": "true" # Use validate first?
        }

        self.logger.debug(f"Kraken Order Data: {data}")
        result = self.kraken_request('/0/private/AddOrder', data)
        self.logger.info(f"Kraken AddOrder Result: {result}")

        if result and not result.get('error'):
            txid = result.get('result', {}).get('txid')
            if txid:
                self.logger.info(f"Kraken order placed successfully for {asset}. TxID: {txid}")
                # --- WAIT FOR FILL ---
                # Market orders usually fill quickly, but check is needed.
                # Use wait_for_kraken_fill logic here.
                filled, avg_price = self.wait_for_kraken_fill(asset, txid[0]) # Assuming txid is list
                if filled:
                     self.logger.info(f"Kraken order for {asset} confirmed filled at avg price: ${avg_price:.4f}")
                     return True, avg_price
            else:
                     self.logger.error(f"Kraken order {txid[0]} for {asset} did not confirm fill within timeout.")
                     # Attempt to cancel? Risky for market orders.
                     return False, None
            else:
                self.logger.error(f"Kraken order placement for {asset} did not return transaction ID.")
                return False, None
        else:
            error_msg = result.get('error', ['Unknown Kraken order error'])
            self.logger.error(f"Kraken order placement failed for {asset}: {error_msg}")
            return False, None


    def wait_for_kraken_fill(self, asset: str, order_txid: str) -> Tuple[bool, Optional[float]]:
        """
        Waits for a Kraken order to be filled by polling its status.
        (Placeholder - Requires Actual Implementation)
        Returns: (is_filled_boolean, filled_price_or_None)
        """
        self.logger.info(f"Waiting for Kraken fill for order {order_txid} ({asset})...")
        start_time = time.time()
        while time.time() - start_time < self.order_timeout:
            try:
                fill_info = self.get_kraken_order_fill_info(asset, order_txid) # Use helper
                if fill_info:
                    status = fill_info.get('status')
                    if status == 'closed': # Order filled (or cancelled manually?)
                         avg_price = fill_info.get('avg_price')
                         vol_exec = fill_info.get('vol_exec')
                         self.logger.info(f"Kraken order {order_txid} closed. Vol Exec: {vol_exec}, Avg Price: {avg_price}")
                         # Check if volume executed matches expected? Important for partial fills.
                         if avg_price is not None and vol_exec is not None and float(vol_exec) > 0:
                              return True, float(avg_price)
                         else: # Closed but no volume executed? (e.g., cancelled before fill)
                              self.logger.warning(f"Kraken order {order_txid} closed but no volume executed or price missing.")
                              return False, None
                    elif status in ['open', 'pending']:
                         self.logger.debug(f"Kraken order {order_txid} status: {status}. Waiting...")
                    else: # e.g., 'expired', 'canceled'
                         self.logger.warning(f"Kraken order {order_txid} has status '{status}'. Fill failed.")
                         return False, None
                else:
                    # Failed to get info, retry
                    self.logger.warning(f"Failed to get fill info for Kraken order {order_txid}, retrying...")

                time.sleep(self.kraken_order_check_interval) # Wait before polling again

            except Exception as e:
                 self.logger.exception(f"Error while waiting for Kraken fill {order_txid}: {e}")
                 time.sleep(self.kraken_order_check_interval) # Wait after error

        self.logger.error(f"Timed out waiting for Kraken order {order_txid} to fill.")
        return False, None

    def get_kraken_order_fill_info(self, asset: str, order_txid: str) -> Optional[Dict[str, Any]]:
         """
         Queries Kraken for the status and fill information of a specific order.
         (Placeholder - Requires Actual Implementation)
         Returns dict with 'status', 'avg_price', 'vol_exec' etc. or None on error.
         """
         self.logger.debug(f"Querying Kraken order info for {order_txid}...")
         data = {
             "nonce": str(int(1000*time.time())),
             "txid": order_txid
         }
         result = self.kraken_request('/0/private/QueryOrders', data)
         # self.logger.debug(f"Kraken QueryOrders Result for {order_txid}: {result}") # Verbose

         if result and not result.get('error') and 'result' in result and order_txid in result['result']:
             order_info = result['result'][order_txid]
             return {
                 "status": order_info.get('status'),
                 "avg_price": order_info.get('price'), # Kraken uses 'price' for average price
                 "vol_exec": order_info.get('vol_exec'),
                 # Add other relevant fields if needed
             }
         else:
             error_msg = result.get('error', ['Unknown QueryOrders error']) if result else ['No response']
             self.logger.error(f"Failed to query Kraken order {order_txid}: {error_msg}")
             return None

    def close_hl_position(self, asset: str) -> bool:
        """Closes the existing Hyperliquid short position for the asset using place_hl_order."""
        self.logger.info(f"Attempting to close HL position for {asset}...")
        if not self.hl_exchange:
             self.logger.error(f"Close HL ({asset}): HL Exchange client not initialized.")
             return False

        # Get current position size from API (more reliable than potentially stale state)
        current_pos_size_qty = self.get_hl_position_size(asset)

        if current_pos_size_qty is None:
             self.logger.error(f"Close HL ({asset}): Failed to get current position size from API.")
             return False
        if abs(current_pos_size_qty) < 1e-9: # Use tolerance for zero check
            self.logger.info(f"Close HL ({asset}): Position already closed or zero size according to API.")
            # Ensure internal state reflects this too (caller should handle comprehensive state update)
            return True # Considered success as the goal state (no position) is met

        if current_pos_size_qty > 0: # Should not happen if we only short
             self.logger.error(f"Close HL ({asset}): Detected unexpected LONG position ({current_pos_size_qty:.8f}). Cannot use close-short logic.")
             return False

        # We have a short position (negative size), need to buy back this quantity
        qty_to_buy = abs(current_pos_size_qty)
        self.logger.info(f"Close HL ({asset}): Current short size is {current_pos_size_qty:.8f}. Placing BUY order for {qty_to_buy:.8f}.")

        # Use place_hl_order with is_entry=False (to indicate buy to close)
        # It handles retries, fill waiting, verification
        # We ignore the returned price here, just need success/failure
        success, _ = self.place_hl_order(asset=asset, is_entry=False, position_size_qty=qty_to_buy)

        if success:
             self.logger.info(f"✅ Close HL ({asset}): BUY order to close short placed and verified filled.")
             # The calling function (e.g., exit_positions) is responsible for
             # updating self.assets[asset]['in_position'] = False and other state.
                    return True
                else:
             self.logger.error(f"❌ Close HL ({asset}): Failed to place/fill/verify BUY order to close short. Position may still be open. Manual intervention likely needed.")
             # Consider triggering emergency alerts/actions here if needed
            return False
    
    def get_hl_position_size(self, asset: str) -> Optional[float]:
        """
        Gets the current position size for an asset from Hyperliquid API.
        (Placeholder - Requires Actual Implementation)
        Returns size as float (negative for short), or None on error.
        """
        self.logger.debug(f"Getting HL position size for {asset}...")
        if not self.hl_info or not self.hl_wallet: return None
        try:
            user_state = self.hl_info.user_state(self.hl_wallet)
            if user_state and 'assetPositions' in user_state:
                asset_positions = user_state['assetPositions']
                # Find the position for the specified asset
                position_info = next((p['position'] for p in asset_positions if p.get('position', {}).get('coin') == asset), None)
                if position_info:
                     # 'szi' is the size, should be negative for short
                     size_str = position_info.get('szi', '0')
                     return float(size_str)
                    else:
                     # No position found for this asset
            return 0.0
            else:
                 self.logger.warning(f"Could not find 'assetPositions' in user_state for {self.hl_wallet}")
                 return None
        except Exception as e:
            self.logger.exception(f"Error getting HL position size for {asset}: {e}")
            return None
    

    def execute_with_cooldown(self, func, *args, **kwargs):
        """(Placeholder - Requires Actual Implementation)"""
        try:
            func(*args, **kwargs)
            self.consecutive_errors = 0 # Reset errors on success
        except Exception as e:
            self.logger.exception(f"Error executing function {func.__name__} with cooldown: {e}")
            self.consecutive_errors += 1
            cooldown = self.extended_cooldown if self.consecutive_errors >= self.max_consecutive_errors else self.error_cooldown
            self.logger.warning(f"Cooling down for {cooldown} seconds due to error.")
            time.sleep(cooldown)

    def emergency_close_all_positions(self):
        """Forcefully attempts to close all active positions on both exchanges."""
        self.logger.critical("!!! EMERGENCY CLOSE ALL POSITIONS INITIATED !!!")
        # Halt new entries immediately if not already done
        # self.halt_new_entries = True 

        # Use a copy of the active positions set for safe iteration
        active_assets_copy = list(self.active_positions)
        
        if not active_assets_copy:
             self.logger.info("Emergency Close: No active positions found to close.")
             return

        self.logger.warning(f"Emergency Closing Positions for: {active_assets_copy}")

        for asset in active_assets_copy:
             self.logger.info(f"Emergency Closing: Processing {asset}...")
             asset_data = self.assets.get(asset)
             if not asset_data:
                  self.logger.error(f"Emergency Closing ({asset}): Asset data not found, skipping.")
                  continue
                 
             # --- Close Hyperliquid Leg --- 
             self.logger.info(f"Emergency Closing ({asset}): Attempting HL close...")
             try:
                  # Use the existing close_hl_position which should handle getting size and placing order
                  # It already contains logging for success/failure.
                  hl_close_success = self.close_hl_position(asset)
                  if not hl_close_success:
                       self.logger.error(f"Emergency Closing ({asset}): close_hl_position reported failure. Manual check REQUIRED.")
                       # Optionally try a market order directly here as a final fallback?
                       # Be cautious of double-closing or unexpected SDK behavior.
             except Exception as e:
                  self.logger.exception(f"Emergency Closing ({asset}): Exception during close_hl_position call: {e}")

             # --- Close Kraken Leg --- 
             self.logger.info(f"Emergency Closing ({asset}): Attempting Kraken close...")
             kraken_qty_to_close = asset_data.get('position_size_kraken_qty')
             if kraken_qty_to_close is None or kraken_qty_to_close <= 1e-9:
                  self.logger.info(f"Emergency Closing ({asset}): No Kraken quantity stored or size is zero, skipping Kraken close.")
            else:
                 try:
                     # Use place_kraken_order (SELL) - it handles retries/fills
                     # For emergency, consider if a market order is faster/more reliable? 
                     # Current place_kraken_order uses limit orders.
                     # For MVP, stick with place_kraken_order, but acknowledge it might fail.
                     kraken_close_success, _ = self.place_kraken_order(asset=asset, is_entry=False, position_size=kraken_qty_to_close)
                     if not kraken_close_success:
                          self.logger.error(f"Emergency Closing ({asset}): place_kraken_order (SELL) reported failure. Manual check REQUIRED.")
                          # Consider attempting a direct market sell via kraken_request as fallback?
                 except Exception as e:
                     self.logger.exception(f"Emergency Closing ({asset}): Exception during place_kraken_order (SELL) call: {e}")

             # --- Update State (Attempt) --- 
             # Even on failure, mark as not in position internally to prevent further automated actions
             # but rely on logs/alerts for manual verification.
             self.logger.warning(f"Emergency Closing ({asset}): Marking asset as closed internally. VERIFY MANUALLY.")
             asset_data['in_position'] = False
             asset_data['last_error'] = f"Emergency Close Triggered {datetime.now()}"
             self.active_positions.discard(asset)
             self.entry_candidates.discard(asset)

        self.logger.critical("!!! EMERGENCY CLOSE ALL POSITIONS COMPLETE - MANUAL VERIFICATION REQUIRED !!!")
        # Consider adding more alerting mechanisms here (e.g., Telegram)

    def check_margin_levels(self):
        """Check margin levels and update available margin"""
        try:
        if not self.hl_info or not self.hl_wallet:
                self.logger.error("Cannot check margin: Hyperliquid Info client or wallet not initialized")
                return

            # Get user state which contains margin info
            user_state = self.hl_info.user_state(self.hl_wallet)
            if not user_state or 'marginSummary' not in user_state:
                self.logger.error("Failed to get user state or margin summary")
                return

            margin_summary = user_state['marginSummary']
            if 'totalRawUsd' in margin_summary:
                self.available_margin = float(margin_summary['totalRawUsd'])
                self.logger.info(f"Updated available margin: ${self.available_margin:.2f}")
            else:
                self.logger.warning(f"Missing totalRawUsd in marginSummary: {margin_summary}")

                except Exception as e:
            self.logger.exception(f"Error checking margin levels: {e}")

    # ========================================================================
    # END OF PASTED/ADAPTED HELPER METHODS
    # ========================================================================


# --- Tier Specific Classes --- (Simplified Wrappers)

class Tier1Bot(ArbBotBase):
    """Tier 1 bot - Max 1 asset, 100% allocation."""
    def __init__(self, config_path=None, user_id=None, selected_tokens=None):
        # Tier 1 specific validation: Ensure only one token is effectively selected
        final_selected_tokens = []
        if selected_tokens:
             # Check against TOKEN_SPECIFICATIONS first
             valid_initial_selection = [t.upper() for t in selected_tokens if t.upper() in TOKEN_SPECIFICATIONS]
             if valid_initial_selection:
                  final_selected_tokens = [valid_initial_selection[0]] # Take the first valid one
                  if len(valid_initial_selection) > 1:
                       print(f"WARNING: Tier 1 Bot initialized with multiple valid tokens ({valid_initial_selection}). Using only the first: {final_selected_tokens[0]}")
             else:
                  print(f"WARNING: Tier 1 Bot initialized, but none of the selected tokens ({selected_tokens}) are valid in trading_pairs.py. Bot may not trade.")
        else:
             print("WARNING: Tier 1 Bot initialized without selected_tokens. Bot may not trade.")

        super().__init__(config_path=config_path, user_id=user_id, selected_tokens=final_selected_tokens, tier=1)
        self.logger.info("Tier 1 Bot Initialized.")
        # Note: Actual asset filtering happens in base class `filter_assets_by_selected_tokens`


class Tier2Bot(ArbBotBase):
    """Tier 2 bot - Max N assets, proportional allocation with pivot."""
    def __init__(self, config_path=None, user_id=None, selected_tokens=None):
        super().__init__(config_path=config_path, user_id=user_id, selected_tokens=selected_tokens, tier=2)
        self.logger.info("Tier 2 Bot Initialized.")


class Tier3Bot(ArbBotBase):
    """Tier 3 bot - Max N assets, proportional allocation with pivot."""
    def __init__(self, config_path=None, user_id=None, selected_tokens=None):
        super().__init__(config_path=config_path, user_id=user_id, selected_tokens=selected_tokens, tier=3)
        self.logger.info("Tier 3 Bot Initialized.")


# --- Example Usage / Entry Point ---

if __name__ == '__main__':
    print("--- Starting Arb Bot Test ---")
    # Specify the path to your configuration file
    # Ensure this file exists and contains [kraken], [hyperliquid], [Strategies] sections
    config_file_path = 'user_config.ini' # MODIFY AS NEEDED

    # Example: Create a dummy config if it doesn't exist for basic testing
    if not os.path.exists(config_file_path):
        print(f"Config file '{config_file_path}' not found. Creating a dummy file.")
        dummy_config = configparser.ConfigParser()
        dummy_config['kraken'] = {'api_key': 'YOUR_KRAKEN_API_KEY', 'api_secret': 'YOUR_KRAKEN_SECRET'}
        dummy_config['hyperliquid'] = {'api_key': 'YOUR_HL_API_KEY_OPTIONAL', 'private_key': 'YOUR_HL_PRIVATE_KEY_0x...', 'wallet_address': 'YOUR_HL_WALLET_ADDRESS_0x...'}
        dummy_config['Strategies'] = {'entry_strategy': 'default', 'exit_strategy': 'exit_abraxas'} # Example strategies
        with open(config_file_path, 'w') as configfile:
            dummy_config.write(configfile)
        print(f"Dummy config created. EDIT '{config_file_path}' with REAL credentials before running.")
        # exit() # Exit after creating dummy to force user edit

    # --- Test Tier 2 ---
    print("--- Testing Tier 2 Bot ---")
    try:
        # Select tokens available in your trading_pairs.py
        test_tokens_t2 = ['BTC', 'ETH'] # MODIFY AS NEEDED
        bot_tier2 = Tier2Bot(config_path=config_file_path, user_id='test_user_t2', selected_tokens=test_tokens_t2)

        # Initialize the bot (loads config, sets up APIs, calculates percentiles)
        bot_tier2.initialize()

        # Start the main run loop if initialization succeeded
        if bot_tier2.running:
            print("Bot initialized successfully. Starting run loop (Press Ctrl+C to stop)...")
            bot_tier2.run()
        else:
            print("Bot initialization failed. Check logs. Exiting.")

    except Exception as e:
        print(f"Error initializing or running Tier 2 Bot: {e}")
        logging.exception("Tier 2 Bot failed") # Log traceback

    # --- Add tests for Tier 1 / Tier 3 similarly if needed ---
    # print("--- Testing Tier 1 Bot ---")
    # try:
    #     test_tokens_t1 = ['BTC'] # MODIFY AS NEEDED
    #     bot_tier1 = Tier1Bot(config_path=config_file_path, user_id='test_user_t1', selected_tokens=test_tokens_t1)
    #     bot_tier1.initialize()
    #     if bot_tier1.running: bot_tier1.run()
    # except Exception as e: print(f"Error T1: {e}"); logging.exception("T1 failed")


    print("--- Arb Bot Test Finished ---")
