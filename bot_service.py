#!/usr/bin/env python3
"""
Bot Service Module for Abraxas Greenprint Funding Bot
------------------------------------------
Manages bot instances for multiple users
"""

import os
import logging
import configparser
import threading
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
import time
import subprocess

from arb_bot import Tier1Bot, Tier2Bot, Tier3Bot
from database import Database
from security import SecurityManager

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class BotService:
    """Service to manage bot instances for multiple users"""
    
    def __init__(self, config_dir: str = "/opt/crypto-arb-bot/config"):
        """
        Initialize bot service
        
        Args:
            config_dir: Directory to store configuration files
        """
        self.config_dir = config_dir
        self.running_bots: Dict[str, Dict[str, Any]] = {}
        self.ensure_config_dir()
        self.db = Database()
        self.security = SecurityManager()
        
    def ensure_config_dir(self):
        """Ensure configuration directory exists with proper permissions"""
        if not os.path.exists(self.config_dir):
            try:
                # Create directory with restricted permissions (only readable by owner)
                os.makedirs(self.config_dir, mode=0o700, exist_ok=True)
                logger.info(f"Created config directory: {self.config_dir}")
            except Exception as e:
                logger.error(f"Failed to create config directory: {str(e)}")
                # Fall back to temporary directory if main directory creation fails
                self.config_dir = "/tmp/arb_bot"
                os.makedirs(self.config_dir, mode=0o700, exist_ok=True)
                logger.warning(f"Using fallback config directory: {self.config_dir}")
        
    def _get_decrypted_keys(self, user_id_str: str) -> Optional[Dict[str, str]]:
        """Helper to retrieve and decrypt API keys from the database."""
        keys = {}
        try:
            telegram_id = int(user_id_str) # DB uses integer ID
            kraken_db_key = self.db.get_api_key(telegram_id, 'kraken')
            hl_db_key = self.db.get_api_key(telegram_id, 'hyperliquid')

            if not kraken_db_key or not hl_db_key:
                logger.error(f"API keys not found in database for user {user_id_str}")
                return None

            keys['kraken_key'] = self.security.decrypt(kraken_db_key.encrypted_key, kraken_db_key.key_iv)
            keys['kraken_secret'] = self.security.decrypt(kraken_db_key.encrypted_secret, kraken_db_key.secret_iv)
            # HL Key might be optional, handle potential decryption errors or missing key
            try:
                keys['hl_key'] = self.security.decrypt(hl_db_key.encrypted_key, hl_db_key.key_iv)
            except Exception:
                 logger.warning(f"Could not decrypt optional HL API key for user {user_id_str}. Using None.")
                 keys['hl_key'] = None # Allow None for optional key
            keys['hl_secret'] = self.security.decrypt(hl_db_key.encrypted_secret, hl_db_key.secret_iv)
            keys['hl_wallet'] = self.db.get_hyperliquid_wallet(telegram_id) # Assuming a DB method to get the wallet address

            if not keys['hl_wallet']:
                logger.error(f"Hyperliquid wallet address not found for user {user_id_str}")
                return None

            return keys
        except ValueError:
             logger.error(f"Invalid user_id format: {user_id_str}. Cannot fetch keys.")
             return None
        except Exception as e:
            logger.exception(f"Error retrieving or decrypting keys for user {user_id_str}: {e}")
            return None

    def create_config(self, user_id_str: str, tier: int, api_keys: Dict[str, str],
                      selected_tokens: List[str], strategies: Dict[str, str]) -> str:
        """
        Create or update configuration file for user. Now includes strategies.
        
        Args:
            user_id_str: User's telegram ID as string.
            tier: Subscription tier.
            api_keys: Dictionary containing decrypted API keys and wallet.
            selected_tokens: List of token symbols to trade.
            strategies: Dictionary with 'entry_strategy' and 'exit_strategy'.
            
        Returns:
            Path to created/updated config file.
        """
        try:
            # Create config parser
            config = configparser.ConfigParser()
            
            # Add API credentials
            config['kraken'] = {
                'api_key': api_keys['kraken_key'],
                'api_secret': api_keys['kraken_secret']
            }
            
            config['hyperliquid'] = {
                'api_key': api_keys.get('hl_key', ''), # Handle potentially missing optional key
                'private_key': api_keys['hl_secret'],
                'wallet_address': api_keys['hl_wallet'] # Include wallet address
            }
            
            # Add service configuration (less critical for arb_bot now, but maybe useful)
            config['service'] = {
                'tier': str(tier),
                'user_id': user_id_str,
                # 'max_slippage': '0.05',
                # 'liquidation_threshold': '0.2'
            }
            
            # Add selected tokens if provided
            if selected_tokens:
                config['tokens'] = {
                    'selected': ','.join(selected_tokens)
                }
            
            # --- Add Strategies Section ---
            config['Strategies'] = {
                'entry_strategy': strategies.get('entry_strategy', 'default'),
                'exit_strategy': strategies.get('exit_strategy', 'default')
            }
            # -----------------------------
            
            # Determine config file path (named with user ID)
            config_path = os.path.join(self.config_dir, f"{user_id_str}.ini")
            
            # Write config with restricted permissions
            with open(config_path, 'w') as f:
                config.write(f)
                
            # Set secure permissions (only readable by owner)
            os.chmod(config_path, 0o600)
            logger.info(f"Configuration file created/updated for user {user_id_str} at {config_path}")
            return config_path
            
        except Exception as e:
            logger.error(f"Failed to create/update config for user {user_id_str}: {str(e)}")
            raise # Re-raise the exception to be caught by the caller
            
    def start_bot_instance(self, user_id_str: str) -> bool:
        """Starts or restarts a bot instance for a given user using data from the database."""
        logger.info(f"Attempting to start bot instance for user {user_id_str}...")
        
        # First, run cleanup_bot.py to ensure no leftover processes
        try:
            logger.info("Running cleanup_bot.py to terminate any existing bot processes...")
            cleanup_result = subprocess.run(
                ["python", "cleanup_bot.py"], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True,
                check=False  # Don't raise exception on non-zero exit
            )
            
            if cleanup_result.returncode == 0:
                logger.info("Cleanup completed successfully")
            else:
                logger.warning(f"Cleanup script returned non-zero exit code: {cleanup_result.returncode}")
                logger.warning(f"Cleanup output: {cleanup_result.stdout}\n{cleanup_result.stderr}")
                # Continue anyway, as we want to try starting the bot even if cleanup had issues
        except Exception as e:
            logger.error(f"Error running cleanup script: {str(e)}")
            # Continue with bot startup attempt
            
        try:
            telegram_id = int(user_id_str)
        except ValueError:
            logger.error(f"Invalid user ID format: {user_id_str}")
            return False

        # --- 1. Fetch User Data from DB ---
        user = self.db.get_user_by_telegram_id(telegram_id)
        if not user:
            logger.error(f"User {user_id_str} not found in database.")
            return False
        if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
             logger.error(f"User {user_id_str} has no active subscription.")
             return False

        tier = user.subscription_tier
        selected_tokens = self.db.get_user_tokens(telegram_id)
        strategies = self.db.get_user_strategies(telegram_id)

        if not selected_tokens:
            logger.error(f"No tokens selected for user {user_id_str}. Cannot start bot.")
            return False

        # --- 2. Fetch and Decrypt API Keys ---
        api_keys = self._get_decrypted_keys(user_id_str)
        if not api_keys:
            logger.error(f"Failed to retrieve or decrypt API keys for user {user_id_str}. Cannot start bot.")
            return False
                
        # --- 3. Create/Update Config File ---
        try:
            config_path = self.create_config(
                user_id_str=user_id_str,
                tier=tier,
                api_keys=api_keys,
                selected_tokens=selected_tokens,
                strategies=strategies
            )
        except Exception as e:
             logger.error(f"Failed to create config file for user {user_id_str}: {e}. Cannot start bot.")
             return False

        # --- 4. Stop Existing Bot (if any) ---
        if user_id_str in self.running_bots:
            logger.info(f"Stopping existing bot for user {user_id_str} before starting new one.")
            self.stop_bot(user_id_str)

        # --- 5. Instantiate Correct Bot Class ---
        bot = None
        BotClass = None
        try:
            if tier == 1: BotClass = Tier1Bot
            elif tier == 2: BotClass = Tier2Bot
            elif tier == 3: BotClass = Tier3Bot
            else:
                logger.error(f"Invalid tier {tier} for user {user_id_str}.")
                return False

            bot = BotClass(
                config_path=config_path,
                user_id=user_id_str,
                selected_tokens=selected_tokens
            )
            logger.info(f"Instantiated {BotClass.__name__} for user {user_id_str}")

        except Exception as e:
             logger.exception(f"Failed to instantiate {BotClass.__name__ if BotClass else 'Bot'} for user {user_id_str}: {e}")
             return False

        # --- 6. Initialize the Bot ---
        try:
            logger.info(f"Initializing bot for user {user_id_str}...")
            bot.initialize()
        except Exception as e:
            logger.exception(f"Error during bot initialization for user {user_id_str}: {e}")
            self.running_bots[user_id_str] = None
            return False

        if not bot.running:
            logger.error(f"Bot initialization failed for user {user_id_str} (bot.running is False). Check arb_bot logs.")
            self.running_bots[user_id_str] = None
            return False

        # --- 7. Start Bot Thread ---
        try:
            bot_thread = threading.Thread(target=bot.run, name=f"ArbBot_{user_id_str}")
            bot_thread.daemon = True
            bot_thread.start()
            
            self.running_bots[user_id_str] = {
                'bot': bot,
                'thread': bot_thread,
                'tier': tier,
                'config_path': config_path,
                'start_time': datetime.now(),
                'selected_tokens': selected_tokens,
                'strategies': strategies
            }
            logger.info(f"Successfully started {BotClass.__name__} for user {user_id_str} with tokens: {', '.join(selected_tokens)}, Strategies: {strategies}")
            self.db.update_bot_status(telegram_id, is_running=True, start_time=datetime.now())
            return True
            
        except Exception as e:
             logger.exception(f"Failed to start bot thread for user {user_id_str}: {e}")
             bot.shutdown(emergency=True)
             self.running_bots[user_id_str] = None
             self.db.update_bot_status(telegram_id, is_running=False, stop_time=datetime.now())
        return False

    def stop_bot(self, user_id_str: str) -> bool:
        """Stop bot for the specified user."""
        success = True
        
        if user_id_str in self.running_bots:
            logger.info(f"Attempting to stop bot for user {user_id_str}...")
            bot_info = self.running_bots.get(user_id_str)
            if not bot_info:
                logger.warning(f"Bot info not found for user {user_id_str} despite being in keys. Inconsistent state?")
                del self.running_bots[user_id_str]
                try: self.db.update_bot_status(int(user_id_str), is_running=False, stop_time=datetime.now())
                except ValueError: pass
                success = True
            else:
                bot = bot_info.get('bot')
                thread = bot_info.get('thread')

                try:
                    if bot:
                        logger.info(f"Calling shutdown() for user {user_id_str}'s bot...")
                        bot.shutdown()
                    else:
                         logger.warning(f"No bot object found for user {user_id_str} during stop request.")

                    if thread and thread.is_alive():
                        logger.info(f"Waiting for bot thread {user_id_str} to join (timeout=60s)...")
                        thread.join(timeout=60)
                        if thread.is_alive():
                            logger.warning(f"Bot thread for user {user_id_str} did not exit cleanly after 60s.")
                        else:
                             logger.info(f"Bot thread for user {user_id_str} joined successfully.")
                    else:
                         logger.info(f"Bot thread for user {user_id_str} was not running or already joined.")

                    del self.running_bots[user_id_str]
                    logger.info(f"Bot instance removed for user {user_id_str}")
                    try: self.db.update_bot_status(int(user_id_str), is_running=False, stop_time=datetime.now())
                    except ValueError: pass
                    success = True
                    
                except Exception as e:
                    logger.exception(f"Error during graceful shutdown for user {user_id_str}: {e}")
                    if user_id_str in self.running_bots:
                        del self.running_bots[user_id_str]
                    try: self.db.update_bot_status(int(user_id_str), is_running=False, stop_time=datetime.now())
                    except ValueError: pass
                    success = False
        else:
            logger.info(f"No bot currently registered as running for user {user_id_str}. Ensuring DB status is updated.")
            try: self.db.update_bot_status(int(user_id_str), is_running=False)
            except ValueError: pass
            success = True
        
        # Run cleanup_bot.py to ensure all processes are terminated
        try:
            logger.info("Running cleanup_bot.py to terminate any remaining bot processes...")
            cleanup_result = subprocess.run(
                ["python", "cleanup_bot.py"], 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True,
                check=False  # Don't raise exception on non-zero exit
            )
            
            if cleanup_result.returncode == 0:
                logger.info("Cleanup completed successfully")
            else:
                logger.warning(f"Cleanup script returned non-zero exit code: {cleanup_result.returncode}")
                logger.warning(f"Cleanup output: {cleanup_result.stdout}\n{cleanup_result.stderr}")
                # Return success based on our own stop logic, not the cleanup script
        except Exception as e:
            logger.error(f"Error running cleanup script: {str(e)}")
            # Return success based on our own stop logic, not the cleanup script
        
        return success

    def get_bot_status(self, user_id_str: str) -> Dict:
        """Get detailed status of a specific bot instance"""
        if user_id_str in self.running_bots:
            bot_info = self.running_bots[user_id_str]
            bot = bot_info.get('bot')
            thread = bot_info.get('thread')
            
            # Get basic status
            status = {
                'user_id': user_id_str,
                'is_running': bool(bot and thread and thread.is_alive() and bot.running),
                'tier': bot_info.get('tier'),
                'start_time': bot_info.get('start_time'),
                'selected_tokens': bot_info.get('selected_tokens'),
                'strategies': bot_info.get('strategies', {'entry_strategy': 'default', 'exit_strategy': 'default'}),
                'thread_alive': bool(thread and thread.is_alive()),
                'bot_running_flag': bool(bot and bot.running)
            }
            
            # Add positions if bot is running
            if bot and bot.running:
                status['positions'] = {}
                status['balances'] = {}
                status['funding_rates'] = {}
                
                # Get positions
                for asset in bot.assets:
                    if bot.assets[asset]['in_position']:
                        status['positions'][asset] = {
                            'hl_position': bot.assets[asset]['hl_position_size'],
                            'kraken_position': bot.assets[asset]['kraken_position_size']
                        }
                
                # Get balances
                try:
                    hl_balance = bot.check_balances().get('hyperliquid', 0)
                    kraken_balance = bot.check_balances().get('kraken', 0)
                    status['balances'] = {
                        'Hyperliquid': hl_balance,
                        'Kraken': kraken_balance
                    }
                except Exception as e:
                    logger.error(f"Error getting balances: {e}")
                
                # Get funding rates
                for asset in bot.assets:
                    if 'ws_funding_rate' in bot.assets[asset] and bot.assets[asset]['ws_funding_rate'] is not None:
                        status['funding_rates'][asset] = bot.assets[asset]['ws_funding_rate'] * 100
                    elif asset in bot.last_funding_rates:
                        status['funding_rates'][asset] = bot.last_funding_rates[asset]
                
                # Get strategies
                if hasattr(bot, 'entry_strategy') and hasattr(bot, 'exit_strategy'):
                    status['strategies'] = {
                        'entry_strategy': bot.entry_strategy,
                        'exit_strategy': bot.exit_strategy
                    }
            
            return status
        else:
            # Check DB for potentially stale status
            db_status = self.db.get_bot_status(user_id_str)
            if db_status and db_status.is_running:
                # Bot is marked as running in DB but not in service
                logger.warning(f"Bot {user_id_str} is marked as running in DB but not in service")
                return {
                    'user_id': user_id_str,
                    'is_running': False,
                    'error': 'Bot has stopped unexpectedly. Please restart with /start_bot',
                    'strategies': {'entry_strategy': 'default', 'exit_strategy': 'default'}
                }
            return {
                'user_id': user_id_str,
                'is_running': False,
                'message': 'Bot is not running. Use /start_bot to start trading',
                'strategies': {'entry_strategy': 'default', 'exit_strategy': 'default'}
            }
        
    def list_active_bots(self) -> Dict[str, Dict[str, Any]]:
        """List all currently active bot instances managed by this service."""
        active_bots_summary = {}
        for user_id, bot_info in list(self.running_bots.items()): # Iterate over copy
            bot = bot_info.get('bot')
            thread = bot_info.get('thread')
            is_running = bool(bot and thread and thread.is_alive() and bot.running)
            if is_running:
                active_bots_summary[user_id] = {
                    'tier': bot_info.get('tier'),
                    'start_time': bot_info.get('start_time'),
                    'selected_tokens': bot_info.get('selected_tokens'),
                    'strategies': bot_info.get('strategies'),
                    'active_positions': list(bot.active_positions) if bot else []
                }
            else:
                 # Cleanup potentially dead bot entries
                 logger.warning(f"Found non-running bot entry for user {user_id}. Cleaning up.")
                 self.stop_bot(user_id) # Attempt graceful cleanup

        return active_bots_summary

    # Helper to get Bot Class - Added for clarity
    def get_bot_class_for_tier(self, tier: int) -> Optional[type]:
        if tier == 1: return Tier1Bot
        elif tier == 2: return Tier2Bot
        elif tier == 3: return Tier3Bot
        else: return None