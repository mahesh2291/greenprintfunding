#!/usr/bin/env python3
"""
Telegram Bot that serves as interface to the Crypto Arbitrage Bot
"""

import os
import logging
import asyncio
import json
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Set, Any
from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    Bot
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    filters
)
from telegram.error import TelegramError, Conflict
from dotenv import load_dotenv
import time
from aiohttp import web

from database import Database, User, APIKey, BotStatus, Transaction, TokenSelection
from security import SecurityManager
from bot_service import BotService
from payment import PaymentManager
from trading_pairs import (
    get_available_pairs, 
    get_available_pairs_list,
    format_pairs_description,
    validate_pair_selection,
    get_active_trading_pairs
)

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    filename='/opt/crypto-arb-bot/logs/bot.log'
)
logger = logging.getLogger(__name__)

# Conversation states
(
    MAIN_MENU,
    AWAITING_TIER,
    AWAITING_TOKEN_SELECTION,
    AWAITING_PAYMENT_METHOD,
    AWAITING_KRAKEN_KEY,
    AWAITING_KRAKEN_SECRET,
    AWAITING_HL_KEY,
    AWAITING_HL_SECRET,
    AWAITING_HL_WALLET, # Add new state for wallet address
    AWAITING_PAYMENT,
    CONFIRM_START,
    CONFIRM_STOP,
    CHOOSING_TIER,
    CHOOSING_TOKENS,
    CHOOSING_ENTRY_STRATEGY,
    CHOOSING_EXIT_STRATEGY,
    CONFIRMING_STRATEGIES,
    CHOOSING_PAYMENT,
    PAYMENT_EMAIL_ENTRY
) = range(19)

class AbraxasGreenprintBot:
    """
    Telegram Bot for Abraxas Greenprint Funding Arbitrage
    """
    def __init__(self, token):
        self.token = token
        self.db = Database()
        self.security = SecurityManager()
        self.bot_service = BotService()
        self.payment = PaymentManager()
        self.logger = logger

        # Set pricing for each tier (USD)
        self.pricing = {
            1: 49,  # $49/month for Tier 1 (1 token)
            2: 95, # $95/month for Tier 2 (2 tokens)
            3: 2500  # $2500/month for Tier 3 (All tokens)
        }
        
        # Token limits per tier
        self.token_limits = {
            1: 1,  # Tier 1: 1 token
            2: 2,  # Tier 2: 2 tokens
            3: 99  # Tier 3: All tokens (setting a high limit)
        }
        
        # Cache available pairs
        self.available_pairs = get_active_trading_pairs()
        
        # Initialize telegram application with webhook settings
        self.application = Application.builder().token(token).build()
        self.setup_handlers()
        
        # Webhook settings
        self.webhook_url = os.getenv('WEBHOOK_URL', 'https://your-domain.com/webhook')
        self.webhook_port = int(os.getenv('WEBHOOK_PORT', '8443'))
        self.webhook_path = f"/webhook/{token}"  # Unique path per bot instance

    def setup_handlers(self):
        """Setup handlers for commands and callbacks"""
        logger.info("Setting up message handlers")
        
        # Define conversation states
        global CHOOSING_TIER, PAYMENT_EMAIL_ENTRY, CHOOSING_PAYMENT, AWAITING_PAYMENT_METHOD, AWAITING_PAYMENT
        global CHOOSING_TOKENS, CHOOSING_ENTRY_STRATEGY, CHOOSING_EXIT_STRATEGY, CONFIRMING_STRATEGIES
        global AWAITING_KRAKEN_KEY, AWAITING_KRAKEN_SECRET, AWAITING_HL_KEY, AWAITING_HL_SECRET
        
        # Add command handlers
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("pairs", self.cmd_pairs))
        self.application.add_handler(CommandHandler("start_bot", self.cmd_start_bot))
        self.application.add_handler(CommandHandler("stop_bot", self.cmd_stop_bot))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        
        # Add subscription conversation handler
        subscribe_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("subscribe", self.cmd_subscribe),
                CallbackQueryHandler(self.choose_tier_callback, pattern=r"^tier_\d$")
            ],
            states={
                CHOOSING_TIER: [
                    CallbackQueryHandler(self.choose_tier_callback, pattern=r"^tier_\d$")
                ],
                CHOOSING_TOKENS: [
                    CallbackQueryHandler(self.token_selection_callback, pattern=r'^(toggle_|save_tokens)')
                ],
                CHOOSING_ENTRY_STRATEGY: [
                    CallbackQueryHandler(self.entry_strategy_callback)
                ],
                CHOOSING_EXIT_STRATEGY: [
                    CallbackQueryHandler(self.exit_strategy_callback)
                ],
                CONFIRMING_STRATEGIES: [
                    CallbackQueryHandler(self.confirm_strategies_callback)
                ],
                CHOOSING_PAYMENT: [
                    CallbackQueryHandler(self.payment_method_callback)
                ],
                AWAITING_PAYMENT_METHOD: [
                    CallbackQueryHandler(self.payment_method_callback)
                ],
                AWAITING_PAYMENT: [
                    CallbackQueryHandler(self.payment_method_callback, pattern=r"^cancel_payment$")
                ],
                PAYMENT_EMAIL_ENTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_payment_email)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_conversation)]
        )
        self.application.add_handler(subscribe_conv_handler)
        
        # Add guided setup handlers - integrated with the strategy selection flow
        tokens_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("tokens", self.cmd_tokens),
                CallbackQueryHandler(self.guide_tokens_callback, pattern="^guide_tokens$")
            ],
            states={
                CHOOSING_TOKENS: [
                    CallbackQueryHandler(self.manage_tokens_callback, pattern=r'^(toggle_|save_tokens)')
                ],
                CHOOSING_ENTRY_STRATEGY: [
                    CallbackQueryHandler(self.entry_strategy_callback)
                ],
                CHOOSING_EXIT_STRATEGY: [
                    CallbackQueryHandler(self.exit_strategy_callback)
                ],
                CONFIRMING_STRATEGIES: [
                    CallbackQueryHandler(self.confirm_strategies_callback)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_conversation)]
        )
        self.application.add_handler(tokens_conv_handler)
        
        # Add callback query handlers for buttons
        self.application.add_handler(CallbackQueryHandler(self.confirm_start_callback, pattern="^confirm_start"))
        self.application.add_handler(CallbackQueryHandler(self.confirm_stop_callback, pattern="^confirm_stop"))
        
        # Add API key setup conversation handler
        logger.info("Setting up API key conversation handler")
        setkeys_conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("setkeys", self.cmd_setkeys),
                CallbackQueryHandler(self.guide_keys_callback, pattern="^guide_keys$")
            ],
            states={
                AWAITING_KRAKEN_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_kraken_key)],
                AWAITING_KRAKEN_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_kraken_secret)],
                AWAITING_HL_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_hl_key)],
                AWAITING_HL_SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_hl_secret)],
                AWAITING_HL_WALLET: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.process_hl_wallet)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel_conversation)]
        )
        logger.info(f"API key conversation handler entry points: {[str(ep) for ep in setkeys_conv_handler.entry_points]}")
        logger.info(f"API key conversation handler states: {list(setkeys_conv_handler.states.keys())}")
        logger.info(f"API key conversation handler state handlers: {[(state, len(handlers)) for state, handlers in setkeys_conv_handler.states.items()]}")
        self.application.add_handler(setkeys_conv_handler)
    
        # Handle unknown commands LAST
        self.application.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))
        
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start command"""
        logger.info(f"Received /start command from user {update.effective_user.id}")
        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name
        
        # Check if user exists in database
        user = self.db.get_user_by_telegram_id(user_id)
        if not user:
            self.db.create_user(telegram_id=user_id, username=username)
            welcome_text = (
                f"👋 *Welcome to the Abraxas Greenprint Funding Bot*, {username}!\n\n"
                "This bot allows you to run automated funding rate arbitrage trading "
                "between Hyperliquid and Kraken exchanges.\n\n"
                "Here's how to get started:\n"
                "1. /subscribe - Choose your subscription tier\n"
                "2. /tokens - Select which tokens to trade\n"
                "3. /setkeys - Configure your exchange API keys\n"
                "4. /start_bot - Start your trading bot\n\n"
                "Type /pairs to see available trading pairs.\n"
                "Type /help for more information."
            )
        else:
            welcome_text = (
                f"Welcome back to Abraxas Greenprint, {username}!\n\n"
                "What would you like to do?\n"
                "/subscribe - Manage your subscription\n"
                "/tokens - Select which tokens to trade\n"
                "/pairs - View available trading pairs\n"
                "/setkeys - Configure API keys\n"
                "/status - Check your bot status\n"
                "/start_bot - Start your bot\n"
                "/stop_bot - Stop your bot\n"
                "/help - Get help"
            )
            
        await update.message.reply_text(welcome_text)
        
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /help command"""
        logger.info(f"Received /help command from user {update.effective_user.id}")
        help_text = (
            "📚 *Abraxas Greenprint Funding Bot Help*\n\n"
            "*Available Commands:*\n"
            "/subscribe - Choose a subscription tier\n"
            "/tokens - Select which tokens to trade\n"
            "/pairs - View available trading pairs\n"
            "/setkeys - Configure exchange API keys\n"
            "/status - Check your bot status\n"
            "/start_bot - Start your trading bot\n"
            "/stop_bot - Stop your trading bot\n"
            "/help - Show this help message\n\n"
            
            "*Subscription Tiers:*\n"
            "🥉 *Tier 1* - $49/month - Trade 1 token of your choice\n"
            "🥈 *Tier 2* - $95/month - Trade 2 tokens of your choice\n"
            "🥇 *Tier 3* - $2500/month - Trade all available tokens\n\n"
            
            "*How Funding Arbitrage Works:*\n"
            "The bot exploits the funding rate differences between perpetual futures "
            "and spot markets by:\n"
            "• Going short on perpetual futures on Hyperliquid\n"
            "• Going long on spot markets on Kraken (or Hyperliquid for HYPE)\n"
            "• Collecting the funding rate while maintaining delta neutrality\n\n"
            
            "*Security Information:*\n"
            "• Your API keys are encrypted using industry standard encryption\n"
            "• Private keys never leave our secure server\n"
            "• You can revoke access anytime by deleting your API keys\n\n"
            
            "*Need more help?*\n"
            "Contact `@admin_username` for support"
        )
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
        
    async def cmd_pairs(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /pairs command to show available trading pairs"""
        # Refresh available pairs
        self.available_pairs = get_active_trading_pairs()
        
        # Format the pairs description
        pairs_text = format_pairs_description()
        
        await update.message.reply_text(pairs_text, parse_mode='Markdown')
        
    async def cmd_subscribe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles subscription command, prompting user to select a subscription tier"""
        user_id = update.effective_user.id
        
        # Check if user already has an active subscription
        user = self.db.get_user_by_telegram_id(user_id)
        
        if user and user.subscription_tier and user.subscription_expiry and user.subscription_expiry > datetime.now():
            remaining_days = (user.subscription_expiry - datetime.now()).days
            
            # User has an active subscription
            await update.message.reply_text(
                f"You already have an active Tier {user.subscription_tier} subscription "
                f"with {remaining_days} days remaining.\n\n"
                f"Would you like to extend your subscription?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Yes, extend subscription", callback_data="subscribe_extend")],
                    [InlineKeyboardButton("No, cancel", callback_data="cancel")]
                ])
            )
            return ConversationHandler.END
        
        # User doesn't have an active subscription, show subscription options
        keyboard = [
            [InlineKeyboardButton(f"Tier 1: $49/month (1 Token)", callback_data="tier_1")],
            [InlineKeyboardButton(f"Tier 2: $95/month (2 Tokens)", callback_data="tier_2")],
            [InlineKeyboardButton(f"Tier 3: $2500/month (All Tokens)", callback_data="tier_3")]
        ]
        
        await update.message.reply_text(
            "Please select a subscription tier:\n\n"
            "🔹 *Tier 1* ($49/month):\n"
            "- Access to base funding bot features\n"
            "- Trade 1 selected token\n"
            "- Basic risk management\n\n"
            
            "🔸 *Tier 2* ($95/month):\n"
            "- All Tier 1 features\n"
            "- Trade 2 selected tokens\n"
            "- Enhanced risk management\n"
            "- Priority support\n\n"
            
            "💎 *Tier 3* ($2500/month):\n"
            "- All Tier 2 features\n"
            "- Trade all available tokens\n"
            "- Advanced risk management\n"
            "- VIP support\n"
            "- Custom strategies",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CHOOSING_TIER
        
    async def choose_tier_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles tier selection and presents payment options"""
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer()
        
        if query.data.startswith("tier_"):
            tier = int(query.data.split("_")[1])
            context.user_data['selected_tier'] = tier
            
            # Store tier selection in context
            if 'payment_data' not in context.user_data:
                context.user_data['payment_data'] = {}
            
            context.user_data['payment_data']['tier'] = tier
            
            # Ask for email first before proceeding to payment methods
            await query.edit_message_text(
                f"You've selected Tier {tier}.\n\n"
                f"Before proceeding with payment, we need your email address to associate with your subscription.\n\n"
                f"Please enter your email address:"
            )
            
            return PAYMENT_EMAIL_ENTRY
        
        return ConversationHandler.END
        
    async def process_payment_email(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process the email address entered by the user"""
        user_id = update.effective_user.id
        email = update.message.text.strip()
        
        # Basic email validation
        import re
        email_pattern = re.compile(r"[^@]+@[^@]+\.[^@]+")
        if not email_pattern.match(email):
            await update.message.reply_text(
                "That doesn't look like a valid email address. Please enter a valid email:"
            )
            return PAYMENT_EMAIL_ENTRY
        
        # Store email in database
        if not self.db.update_user_email(user_id, email):
            # If user doesn't exist, create them first
            self.db.create_user(user_id, update.effective_user.username)
            self.db.update_user_email(user_id, email)
        
        # Store email in context for later use
        if 'payment_data' not in context.user_data:
            context.user_data['payment_data'] = {}
        context.user_data['payment_data']['email'] = email
        
        # Now show payment method options
        tier = context.user_data.get('selected_tier', 1)
        
        # Set the appropriate amount based on tier
        amount = self.pricing.get(tier, 49) # Use the pricing dict, default to Tier 1 price
        
        context.user_data['payment_data']['amount'] = amount
        
        # Show payment options
        keyboard = [
            [InlineKeyboardButton("Pay with BoomFi (Crypto)", callback_data="pay_boomfi")]
        ]
        
        await update.message.reply_text(
            f"Thanks! We'll use {email} for your subscription.\n\n"
            f"You've selected Tier {tier} subscription: ${amount}/month.\n\n"
            f"Please choose your payment method:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CHOOSING_PAYMENT
        
    async def token_selection_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle token selection callback"""
        query = update.callback_query
        await query.answer()
        
        # For our updates from the cmd_tokens method, we should use manage_tokens_callback
        if query.data.startswith("toggle_") or query.data == "save_tokens":
            return await self.manage_tokens_callback(update, context)
            
        # Rest of the existing token_selection_callback implementation for subscription flow
        user_id = query.from_user.id
        selected_tier = context.user_data.get('selected_tier', 1)
        max_tokens = self.token_limits[selected_tier]
        
        if 'selected_tokens' not in context.user_data:
            context.user_data['selected_tokens'] = []
            
        if query.data == "tokens_done":
            # User is done selecting tokens
            if not context.user_data['selected_tokens']:
                # No tokens selected, ask user to select at least one
                await query.edit_message_text(
                    "Please select at least one token before proceeding.",
                    reply_markup=query.message.reply_markup
                )
                return CHOOSING_TOKENS
                
            # Format the selected tokens for display
            tokens_text = ", ".join(context.user_data['selected_tokens'])
            
            # Move to entry strategy selection instead of payment
            await query.edit_message_text(
                f"Great! You've selected: *{tokens_text}* for your Tier {selected_tier} subscription.\n\n"
                "Now, choose your *entry strategy*:",
                parse_mode='Markdown',
                reply_markup=self.get_entry_strategy_keyboard()
            )
            
            return CHOOSING_ENTRY_STRATEGY
            
        elif query.data.startswith("token_"):
            # User selected a token
            token = query.data.split("_")[1]
            
            # Check if token is already selected
            if token in context.user_data['selected_tokens']:
                # Remove token
                context.user_data['selected_tokens'].remove(token)
            else:
                # Check if max tokens reached
                if len(context.user_data['selected_tokens']) >= max_tokens:
                    await query.answer(f"Maximum {max_tokens} tokens for Tier {selected_tier}. Deselect a token first.")
                    return CHOOSING_TOKENS
                    
                # Add token
                context.user_data['selected_tokens'].append(token)
                
            # Update button appearance based on selection
            token_buttons = []
            for available_token in self.available_pairs:
                if available_token in context.user_data['selected_tokens']:
                    # Mark as selected
                    token_buttons.append([
                        InlineKeyboardButton(f"✅ {available_token}", callback_data=f"token_{available_token}")
                    ])
                else:
                    token_buttons.append([
                        InlineKeyboardButton(available_token, callback_data=f"token_{available_token}")
                    ])
                    
            token_buttons.append([
                InlineKeyboardButton("Done Selecting", callback_data="tokens_done")
            ])
            
            # Show current selections
            selected_text = ", ".join(context.user_data['selected_tokens']) if context.user_data['selected_tokens'] else "None"
            
            await query.edit_message_text(
                f"Tier {selected_tier} - ${self.pricing[selected_tier]}/month\n\n"
                f"This tier allows you to select up to {max_tokens} tokens.\n\n"
                f"*Currently selected*: {selected_text}\n"
                f"({len(context.user_data['selected_tokens'])}/{max_tokens})",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(token_buttons)
            )
            
            return CHOOSING_TOKENS
            
        # After token selection is complete, move to entry strategy selection
        if "done" in query.data:
            # Save selected tokens
            selected_tokens = context.user_data.get('selected_tokens', [])
            if not selected_tokens:
                await query.edit_message_text("You must select at least one token to continue. Please try again.")
                return ConversationHandler.END
                
            tier = context.user_data.get('selected_tier')
            
            # Display entry strategy selection
            await query.edit_message_text(
                f"Great! You've selected {', '.join(selected_tokens)} for your Tier {tier} subscription.\n\n"
                "Now, choose your *entry strategy*:",
                parse_mode="Markdown",
                reply_markup=self.get_entry_strategy_keyboard()
            )
            return CHOOSING_ENTRY_STRATEGY
        
    def get_entry_strategy_keyboard(self):
        """Create keyboard with entry strategy options"""
        keyboard = [
            [InlineKeyboardButton("Default Entry Strategy (enter above 60th percentile)", callback_data="entry_default")],
            [InlineKeyboardButton("Enter above 50th percentile", callback_data="entry_50")],
            [InlineKeyboardButton("Enter above 75th percentile", callback_data="entry_75")],
            [InlineKeyboardButton("Enter above 85th percentile", callback_data="entry_85")],
            [InlineKeyboardButton("Enter above 95th percentile", callback_data="entry_95")],
            [InlineKeyboardButton("Abraxas Optimized Funding", callback_data="entry_abraxas")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def get_exit_strategy_keyboard(self):
        """Create keyboard with exit strategy options"""
        keyboard = [
            [InlineKeyboardButton("Default Exit Strategy (sell before next negative rate)", callback_data="exit_default")],
            [InlineKeyboardButton("Exit below 50th percentile", callback_data="exit_50")],
            [InlineKeyboardButton("Exit below 35th percentile", callback_data="exit_35")],
            [InlineKeyboardButton("Exit below 20th percentile", callback_data="exit_20")],
            [InlineKeyboardButton("Exit below 10th percentile", callback_data="exit_10")],
            [InlineKeyboardButton("Abraxas Optimized Exit", callback_data="exit_abraxas")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    def get_strategies_confirmation_keyboard(self):
        """Create keyboard for confirming strategy selections"""
        keyboard = [
            [InlineKeyboardButton("Confirm Strategies", callback_data="confirm_strategies")],
            [InlineKeyboardButton("Change Entry Strategy", callback_data="change_entry")],
            [InlineKeyboardButton("Change Exit Strategy", callback_data="change_exit")]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    async def entry_strategy_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle entry strategy selection"""
        query = update.callback_query
        await query.answer()
        
        # Extract the selected entry strategy
        strategy_data = query.data
        
        # Map the callback data to descriptive text
        strategy_descriptions = {
            "entry_default": "Default (enter above 60th percentile)",
            "entry_50": "Enter above 50th percentile",
            "entry_75": "Enter above 75th percentile",
            "entry_85": "Enter above 85th percentile",
            "entry_95": "Enter above 95th percentile",
            "entry_abraxas": "Abraxas Optimized Funding (enter above 60th percentile)"
        }
        
        # Store the selected strategy
        context.user_data['entry_strategy'] = strategy_data.replace("entry_", "")
        context.user_data['entry_strategy_desc'] = strategy_descriptions.get(strategy_data)
        
        # Display exit strategy selection
        await query.edit_message_text(
            f"Entry strategy selected: *{strategy_descriptions.get(strategy_data)}*\n\n"
            "Now, choose your *exit strategy*:",
            parse_mode="Markdown",
            reply_markup=self.get_exit_strategy_keyboard()
        )
        return CHOOSING_EXIT_STRATEGY
    
    async def exit_strategy_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle exit strategy selection"""
        query = update.callback_query
        await query.answer()
        
        # Extract the selected exit strategy
        strategy_data = query.data
        
        # Map the callback data to descriptive text
        strategy_descriptions = {
            "exit_default": "Default (sell before next negative rate)",
            "exit_50": "Exit below 50th percentile",
            "exit_35": "Exit below 35th percentile",
            "exit_20": "Exit below 20th percentile",
            "exit_10": "Exit below 10th percentile",
            "exit_abraxas": "Abraxas Optimized Exit (sell before next negative rate)"
        }
        
        # Store the selected strategy
        context.user_data['exit_strategy'] = strategy_data.replace("exit_", "")
        context.user_data['exit_strategy_desc'] = strategy_descriptions.get(strategy_data)
        
        # Display confirmation of selected strategies
        entry_desc = context.user_data.get('entry_strategy_desc')
        exit_desc = strategy_descriptions.get(strategy_data)
        
        await query.edit_message_text(
            "*Strategy Selection Summary*\n\n"
            f"*Entry Strategy:* {entry_desc}\n"
            f"*Exit Strategy:* {exit_desc}\n\n"
            "Are you satisfied with these selections?",
            parse_mode="Markdown",
            reply_markup=self.get_strategies_confirmation_keyboard()
        )
        return CONFIRMING_STRATEGIES
    
    async def confirm_strategies_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle strategy confirmation"""
        query = update.callback_query
        await query.answer()
        
        action = query.data
        
        if action == "change_entry":
            # Go back to entry strategy selection
            await query.edit_message_text(
                "Select your entry strategy:",
                reply_markup=self.get_entry_strategy_keyboard()
            )
            return CHOOSING_ENTRY_STRATEGY
            
        elif action == "change_exit":
            # Go back to exit strategy selection
            await query.edit_message_text(
                "Select your exit strategy:",
                reply_markup=self.get_exit_strategy_keyboard()
            )
            return CHOOSING_EXIT_STRATEGY
            
        elif action == "confirm_strategies":
            # Check if we're in the initial subscription flow or post-subscription token selection
            user_id = query.from_user.id
            user = self.db.get_user_by_telegram_id(user_id)
            
            # Save the selected strategies to the database
            entry_strategy = context.user_data.get('entry_strategy', 'default')
            exit_strategy = context.user_data.get('exit_strategy', 'default')
            self.db.update_user_strategies(user_id, entry_strategy, exit_strategy)
            
            # Check if user is in initial subscription flow by looking for payment_data
            if 'payment_data' in context.user_data:
                # Continue to payment method selection (subscription flow)
                tier = context.user_data.get('selected_tier')
                selected_tokens = context.user_data.get('selected_tokens', [])
                entry_strategy_desc = context.user_data.get('entry_strategy_desc')
                exit_strategy_desc = context.user_data.get('exit_strategy_desc')
                
                # Create payment options
                payment_keyboard = [
                    [
                        InlineKeyboardButton("Pay with BTC", callback_data=f"pay_BTC_{tier}"),
                    ],
                    [
                        InlineKeyboardButton("Pay with SOL", callback_data=f"pay_SOL_{tier}"),
                    ],
                    [
                        InlineKeyboardButton("Pay with USDC", callback_data=f"pay_USDC_{tier}"),
                    ],
                    [
                        InlineKeyboardButton("Cancel", callback_data="cancel_payment")
                    ]
                ]
                
                # Prepare for payment
                await query.edit_message_text(
                    f"*Subscription Summary*\n\n"
                    f"*Tier:* {tier}\n"
                    f"*Selected Tokens:* {', '.join(selected_tokens)}\n"
                    f"*Entry Strategy:* {entry_strategy_desc}\n"
                    f"*Exit Strategy:* {exit_strategy_desc}\n\n"
                    f"Total: *${self.pricing[tier]}/month*\n\n"
                    "Please choose your payment method:",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(payment_keyboard)
                )
                return CHOOSING_PAYMENT
            else:
                # Post-subscription flow - prompt for API key setup
                # Get display names for strategies
                entry_strategy_desc = context.user_data.get('entry_strategy_desc', 'Default')
                exit_strategy_desc = context.user_data.get('exit_strategy_desc', 'Default')
                
                # Offer options for next steps
                keyboard = [
                    [InlineKeyboardButton("Set up API Keys", callback_data="guide_keys")],
                    [InlineKeyboardButton("Start Bot", callback_data="confirm_start")]
                ]
                
                # Check if API keys are already set up
                kraken_key = self.db.get_api_key(user_id, 'kraken')
                hl_key = self.db.get_api_key(user_id, 'hyperliquid')
                
                if not kraken_key or not hl_key:
                    # User needs to set up API keys
                    await query.edit_message_text(
                        f"✅ Strategies saved successfully!\n\n"
                        f"*Entry Strategy:* {entry_strategy_desc}\n"
                        f"*Exit Strategy:* {exit_strategy_desc}\n\n"
                        f"Next step: Let's set up your API keys so you can start trading.",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Set up API Keys", callback_data="guide_keys")]
                        ])
                    )
                else:
                    # API keys are already set up, offer to start the bot
                    await query.edit_message_text(
                        f"✅ Strategies saved successfully!\n\n"
                        f"*Entry Strategy:* {entry_strategy_desc}\n"
                        f"*Exit Strategy:* {exit_strategy_desc}\n\n"
                        f"Your bot is configured and ready. You can now start trading!",
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Start Bot", callback_data="confirm_start")]
                        ])
                    )
                
                return ConversationHandler.END
        
    async def payment_method_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles payment method selection"""
        query = update.callback_query
        user_id = query.from_user.id
        
        await query.answer()
        
        # Get payment data from context
        payment_data = context.user_data.get('payment_data', {})
        tier = payment_data.get('tier', 1)
        amount = payment_data.get('amount', 50)
        email = payment_data.get('email', '')
        
        if not email:
            # If email is missing, get it from the database
            user = self.db.get_user_by_telegram_id(user_id)
            email = user.email if user and user.email else ''
        
        if query.data == "pay_boomfi":
            payment_manager = PaymentManager()
            
            # Generate payment request
            payment_request = payment_manager.generate_payment_request(
                user_id=user_id,
                amount=amount,
                tier=tier
            )
            
            if payment_request.get('success'):
                payment_id = payment_request.get('payment_id')
                checkout_url = payment_request.get('checkout_url')
                
                # Store transaction in database
                self.db.create_transaction(
                    user_id=user_id,
                    amount=amount,
                    tier=tier,
                    transaction_id=payment_id,
                    payment_data=json.dumps(payment_request)
                )
                
                # Send payment instructions
                email_reminder = f"**IMPORTANT**: Please use the email address '{email}' during checkout so we can correctly match your payment."
                
                await query.edit_message_text(
                    f"Please complete your payment for Tier {tier} (${amount}):\n\n"
                    f"{email_reminder}\n\n"
                    f"1. Click this link to pay: [Complete Payment]({checkout_url})\n"
                    f"2. Follow the instructions on the payment page.\n"
                    f"3. Your subscription will be activated once payment is confirmed.\n\n"
                    f"Payment ID: `{payment_id}`\n\n"
                    f"⏳ *We'll notify you here once your payment is confirmed.*",
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True
                )
                
                # Don't start background task to check payment status immediately
                # We'll rely on the webhook for payment confirmation
            else:
                error = payment_request.get('error', 'Unknown error')
                await query.edit_message_text(
                    f"Sorry, there was an error generating your payment request: {error}\n\n"
                    f"Please try again later or contact support."
                )
        else:
            await query.edit_message_text(
                "Invalid payment method selected. Please try again."
            )
        
        return ConversationHandler.END
        
    async def handle_payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback after user returns from BoomFi payment page"""
        # This is called when user returns from payment page with /start payment_confirmed_XXXXX
        # Extract payment ID from the command
        message = update.message.text
        parts = message.split("_", 2)
        
        if len(parts) < 3:
            await update.message.reply_text(
                "Invalid payment callback. Please use /start to begin."
            )
            return ConversationHandler.END
            
        payment_id = parts[2]
        self.logger.info(f"Payment callback received for payment {payment_id}")
        
        # Check payment status in database
        transaction_data = self.db.get_transaction_with_user(payment_id)
        
        if not transaction_data:
            await update.message.reply_text(
                "Payment not found. Please use /subscribe to start a new subscription."
            )
            return ConversationHandler.END
            
        user_id = transaction_data['user_id']
        status = transaction_data['status']
        tier = transaction_data['tier']
        
        # Save the user's strategy selections if they exist
        if context.user_data.get('entry_strategy') and context.user_data.get('exit_strategy'):
            entry_strategy = context.user_data.get('entry_strategy')
            exit_strategy = context.user_data.get('exit_strategy')
            
            # Update user's strategy preferences in the database
            self.db.update_user_strategies(user_id, entry_strategy, exit_strategy)
            self.logger.info(f"Updated strategies for user {user_id}: entry={entry_strategy}, exit={exit_strategy}")
        
        if status == 'completed':
            # Payment is already verified
            await update.message.reply_text(
                f"✅ Your payment has been confirmed and your Tier {tier} subscription is active.\n\n"
                f"Let's set up your API keys so you can start trading.\n\n"
                f"Please enter your Kraken API Key:"
            )
            return AWAITING_KRAKEN_KEY
        else:
            # Payment is still pending - webhook may not have been received yet
            # Start the verification task to check
            asyncio.create_task(self.verify_payment_task(user_id, payment_id))
            
            # Tell the user we're verifying
            await update.message.reply_text(
                "🔄 We're verifying your cryptocurrency payment. This may take a few moments...\n\n"
                "You will receive a notification once your payment is confirmed."
            )
            
            return ConversationHandler.END
        
    async def cmd_tokens(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /tokens command to manage token selections"""
        user_id = update.effective_user.id
        user = self.db.get_user_by_telegram_id(user_id)
        
        if not user:
            # Handle direct command
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    "Please use /start to register first."
                )
            # Handle callback from inline button
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    "Please use /start to register first."
                )
            return ConversationHandler.END
            
        # Check if user has an active subscription
        if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
            # Handle direct command
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    "You don't have an active subscription. Please subscribe first with /subscribe"
                )
            # Handle callback from inline button
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    "You don't have an active subscription. Please subscribe first with /subscribe"
                )
            return ConversationHandler.END
            
        # Get current token selections
        current_tokens = self.db.get_user_tokens(user_id)
        max_tokens = self.token_limits[user.subscription_tier]
        
        # Refresh available pairs
        self.available_pairs = get_active_trading_pairs()
        
        # Create token selection UI
        token_buttons = []
        for token in self.available_pairs:
            if token in current_tokens:
                token_buttons.append([
                    InlineKeyboardButton(f"✅ {token}", callback_data=f"toggle_{token}")
                ])
            else:
                token_buttons.append([
                    InlineKeyboardButton(token, callback_data=f"toggle_{token}")
                ])
        
        token_buttons.append([
            InlineKeyboardButton("Confirm Token Selection", callback_data="save_tokens")
        ])
        
        # Store current tokens in context
        context.user_data['current_tokens'] = current_tokens.copy()
        
        # Format current selections text
        current_text = ", ".join(current_tokens) if current_tokens else "None"
        
        message_text = (
            f"*Token Selection*\n\n"
            f"Your subscription (Tier {user.subscription_tier}) allows you to select "
            f"{'1 token' if user.subscription_tier == 1 else '2 tokens' if user.subscription_tier == 2 else 'all available tokens'}.\n\n"
            f"*Current selections:* {current_text}\n"
            f"({len(current_tokens)}/{max_tokens})\n\n"
            f"Tap on tokens to select or deselect them. There are {len(self.available_pairs)} tokens available:"
        )
        
        # Handle direct command
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(
                message_text,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(token_buttons)
            )
        # Handle callback from inline button
        elif hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(
                message_text,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(token_buttons)
            )
        
        # Make sure we're clearing any existing conversations
        return CHOOSING_TOKENS
        
    async def manage_tokens_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle token management callbacks"""
        query = update.callback_query
        user_id = query.from_user.id

        logger.info(f"Starting token management for user {user_id}")
        logger.info(f"Callback data: {query.data}")
        await query.answer()
        
        try:
            # Get user from database
            user = self.db.get_user_by_telegram_id(user_id)
            if not user:
                logger.warning(f"User {user_id} not found in database during token management")
                await query.edit_message_text("User not found. Please use /start to register.")
                return ConversationHandler.END
            
            logger.info(f"Found user {user_id} with tier {user.subscription_tier}")

            max_tokens = self.token_limits[user.subscription_tier]
            logger.info(f"User {user_id} has token limit of {max_tokens} for tier {user.subscription_tier}")
        
            if query.data == "save_tokens":
                logger.info(f"User {user_id} attempting to save token selections")
                # Save the current token selections
                if 'current_tokens' in context.user_data:
                    tokens = context.user_data['current_tokens']
                    logger.info(f"User {user_id} selected tokens: {tokens}")

                    # Validate token count against tier limit
                    if len(tokens) > max_tokens:
                        logger.warning(f"User {user_id} attempted to save {len(tokens)} tokens, exceeding limit of {max_tokens}")
                        await query.edit_message_text(
                            f"❌ You can only select {max_tokens} token{'s' if max_tokens > 1 else ''} for Tier {user.subscription_tier}.\n"
                            "Please deselect some tokens and try again."
                        )
                        return CHOOSING_TOKENS

                    success = self.db.update_user_tokens(user_id, tokens)
                    logger.info(f"Token update for user {user_id} {'succeeded' if success else 'failed'}")
                
                    if success:
                        # Store selected tokens for use in the strategy selection
                        context.user_data['selected_tokens'] = tokens.copy()
                        logger.info(f"Stored {len(tokens)} tokens in context for user {user_id}")
                    
                        # Show confirmation message
                        await query.edit_message_text(
                            f"✅ *Token Selection Confirmed!*\n\n"
                            f"*Selected Tokens:* {', '.join(tokens) if tokens else 'None'}\n\n"
                            f"Now, let's configure your *trading strategies*.\n\n"
                            f"Choose your *entry strategy*:",
                            parse_mode='Markdown',
                            reply_markup=self.get_entry_strategy_keyboard()
                        )
                        logger.info(f"Transitioning user {user_id} to strategy selection")
                        return CHOOSING_ENTRY_STRATEGY
                    else:
                        logger.error(f"Failed to update tokens in database for user {user_id}")
                        await query.edit_message_text(
                            "❌ There was a problem saving your token selections. Please try again."
                        )
                    
                        # Clear token selection state
                        if 'current_tokens' in context.user_data:
                            del context.user_data['current_tokens']
                        if 'in_token_selection' in context.user_data:  
                            del context.user_data['in_token_selection']
                        
                        return ConversationHandler.END
                else:
                    logger.warning(f"No token selections found in context for user {user_id}")
                    await query.edit_message_text(
                        "⚠️ No token selections found. Please try again."
                    )
                    return ConversationHandler.END
            
            elif query.data.startswith("toggle_"):
                # Toggle a token selection
                token = query.data.split("_")[1]
                logger.info(f"User {user_id} toggling token: {token}")
            
                if 'current_tokens' not in context.user_data:
                    context.user_data['current_tokens'] = []
                    logger.info(f"Initialized empty token selection for user {user_id}")
                
                current_tokens = context.user_data['current_tokens']
            
                # Toggle the token
                if token in current_tokens:
                    current_tokens.remove(token)
                    logger.info(f"User {user_id} removed token: {token}")
                else:
                    # Check if max tokens reached
                    if len(current_tokens) >= max_tokens:
                        logger.warning(f"User {user_id} attempted to exceed token limit of {max_tokens}")
                        await query.answer(f"Maximum {max_tokens} tokens for Tier {user.subscription_tier}. Deselect a token first.")
                        return CHOOSING_TOKENS
                    
                    current_tokens.append(token)
                    logger.info(f"User {user_id} added token: {token}")
                
                # Update UI
                token_buttons = []
                for available_token in self.available_pairs:
                    if available_token in current_tokens:
                        token_buttons.append([
                            InlineKeyboardButton(f"✅ {available_token}", callback_data=f"toggle_{available_token}")
                        ])
                    else:
                        token_buttons.append([
                            InlineKeyboardButton(available_token, callback_data=f"toggle_{available_token}")
                        ])
                    
                token_buttons.append([
                    InlineKeyboardButton("Confirm Token Selection", callback_data="save_tokens")
                ])
            
                # Format current selections text
                current_text = ", ".join(current_tokens) if current_tokens else "None"
            
                try:
                    await query.edit_message_text(
                        f"*Token Selection*\n\n"
                        f"Your subscription (Tier {user.subscription_tier}) allows you to select "
                        f"{'1 token' if user.subscription_tier == 1 else '2 tokens' if user.subscription_tier == 2 else 'all available tokens'}.\n\n"
                        f"*Current selections:* {current_text}\n"
                        f"({len(current_tokens)}/{max_tokens})\n\n"
                        f"Tap on tokens to select or deselect them. There are {len(self.available_pairs)} tokens available:",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup(token_buttons)
                    )
                    logger.info(f"Successfully updated token selection UI for user {user_id}")
                except Exception as edit_error:
                    logger.error(f"Failed to edit message for user {user_id}: {str(edit_error)}")
                    # If editing fails, send a new message
                    await query.message.reply_text(
                        f"*Token Selection*\n\n"
                        f"Your subscription (Tier {user.subscription_tier}) allows you to select "
                        f"{'1 token' if user.subscription_tier == 1 else '2 tokens' if user.subscription_tier == 2 else 'all available tokens'}.\n\n"
                        f"*Current selections:* {current_text}\n"
                        f"({len(current_tokens)}/{max_tokens})\n\n"
                        f"Tap on tokens to select or deselect them. There are {len(self.available_pairs)} tokens available:",
                        parse_mode='Markdown',
                        reply_markup=InlineKeyboardMarkup(token_buttons)
                    )
                    logger.info(f"Sent new message to user {user_id} after edit failed")
                
                return CHOOSING_TOKENS
            
        except Exception as e:
            logger.error(f"Unexpected error in manage_tokens_callback for user {user_id}: {str(e)}")
            logger.error(f"Error details: {type(e).__name__}: {str(e)}")
            try:
                await query.edit_message_text(
                    "❌ An error occurred while managing token selections. Please try again."
                )
            except:
                pass
            return ConversationHandler.END
        
    async def cmd_setkeys(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /setkeys command"""
        user_id = update.effective_user.id
        user = self.db.get_user_by_telegram_id(user_id)
        
        if not user:
            # Handle direct command
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    "Please use /start to register first."
                )
            # Handle callback from inline button
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    "Please use /start to register first."
                )
            return ConversationHandler.END
            
        # Check if user has an active subscription
        if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
            # Handle direct command
            if hasattr(update, 'message') and update.message:
                await update.message.reply_text(
                    "You don't have an active subscription. Please subscribe first with /subscribe"
                )
            # Handle callback from inline button
            elif hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.message.reply_text(
                    "You don't have an active subscription. Please subscribe first with /subscribe"
                )
            return ConversationHandler.END
            
        message_text = (
            "🔑 Let's set up your API keys.\n\n"
            "⚠️ *IMPORTANT*: Never share your API keys with anyone else!\n\n"
            "First, please enter your Kraken API Key:"
        )
        
        # Handle direct command
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(
                message_text,
                parse_mode='Markdown'
            )
        # Handle callback from inline button
        elif hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.message.reply_text(
                message_text,
                parse_mode='Markdown'
            )
        
        # Make sure we're clearing any existing conversations and starting fresh
        return AWAITING_KRAKEN_KEY
        
    async def process_kraken_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process Kraken API key input"""
        try:
            logger.info(f"Processing Kraken API key from user {update.effective_user.id}")
            
            # Delete the message containing the API key for security
            await update.message.delete()
            
            # Store the API key securely
            context.user_data['kraken_key'] = update.message.text
            
            # Send a confirmation message
            msg = await update.message.reply_text(
                "✅ Kraken API Key received.\n\n"
                "Now, please enter your Kraken API Secret:"
            )
            
            logger.info(f"Transitioning to AWAITING_KRAKEN_SECRET state for user {update.effective_user.id}")
            return AWAITING_KRAKEN_SECRET
        except Exception as e:
            logger.error(f"Error processing Kraken API key: {str(e)}")
            await update.message.reply_text(
                "❌ There was an error processing your API key. Please try again with /setkeys"
            )
            return ConversationHandler.END
        
    async def process_kraken_secret(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process Kraken API secret input"""
        try:
            logger.info(f"Processing Kraken API secret from user {update.effective_user.id}")
            
            # Delete the message containing the API secret for security
            await update.message.delete()
            
            # Store the API secret securely
            context.user_data['kraken_secret'] = update.message.text
            
            # Send a confirmation message
            msg = await update.message.reply_text(
                "✅ Kraken API Secret received.\n\n"
                "Now, please enter your Hyperliquid API Key:"
            )
            
            logger.info(f"Transitioning to AWAITING_HL_KEY state for user {update.effective_user.id}")
            return AWAITING_HL_KEY
        except Exception as e:
            logger.error(f"Error processing Kraken API secret: {str(e)}")
            await update.message.reply_text(
                "❌ There was an error processing your API secret. Please try again with /setkeys"
            )
            return ConversationHandler.END
        
    async def process_hl_key(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process Hyperliquid API key input"""
        try:
            logger.info(f"Processing Hyperliquid API key from user {update.effective_user.id}")
            
            # Delete the message containing the API key for security
            await update.message.delete()
            
            # Store the API key securely
            context.user_data['hl_key'] = update.message.text
            
            # Send a confirmation message
            msg = await update.message.reply_text(
                "✅ Hyperliquid API Key received.\n\n"
                "Finally, please enter your Hyperliquid API Secret:"
            )
            
            logger.info(f"Transitioning to AWAITING_HL_SECRET state for user {update.effective_user.id}")
            return AWAITING_HL_SECRET
        except Exception as e:
            logger.error(f"Error processing Hyperliquid API key: {str(e)}")
            await update.message.reply_text(
                "❌ There was an error processing your API key. Please try again with /setkeys"
            )
            return ConversationHandler.END
        
    async def process_hl_secret(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process Hyperliquid API secret input"""
        try:
            logger.info(f"Processing Hyperliquid API secret from user {update.effective_user.id}")

            # Delete the message containing the API secret for security
            await update.message.delete()

            # Store the API secret securely temporarily
            context.user_data['hl_secret'] = update.message.text

            # Send a confirmation message and ask for wallet address
            msg = await update.message.reply_text(
                "✅ Hyperliquid API Secret received.\n\n"
                "Finally, please enter your Hyperliquid Wallet Address (starting with 0x):"
            )

            logger.info(f"Transitioning to AWAITING_HL_WALLET state for user {update.effective_user.id}")
            return AWAITING_HL_WALLET # Transition to ask for wallet address

        except Exception as e:
            logger.error(f"Error processing Hyperliquid API secret: {str(e)}")
            await update.message.reply_text(
                "❌ There was an error processing your API secret. Please try again with /setkeys"
            )
            # Clear potentially stored keys if error occurs
            if 'kraken_key' in context.user_data: del context.user_data['kraken_key']
            if 'kraken_secret' in context.user_data: del context.user_data['kraken_secret']
            if 'hl_key' in context.user_data: del context.user_data['hl_key']
            if 'hl_secret' in context.user_data: del context.user_data['hl_secret']
            return ConversationHandler.END

    async def process_hl_wallet(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Process Hyperliquid wallet address input and save all keys."""
        try:
            user_id = update.effective_user.id
            logger.info(f"Processing Hyperliquid Wallet Address from user {user_id}")

            # Delete the message containing the wallet address for security
            await update.message.delete()

            # Validate and store the wallet address
            hl_wallet_address = update.message.text.strip()
            if not hl_wallet_address.startswith('0x') or len(hl_wallet_address) < 40: # Basic validation
                 await update.message.reply_text(
                    "❌ Invalid Hyperliquid wallet address format. It should start with '0x'. "
                    "Please start the API key setup again with /setkeys."
                 )
                 # Clear context data on failure
                 context.user_data.clear()
                 return ConversationHandler.END

            context.user_data['hl_wallet'] = hl_wallet_address

            # Get all API keys and wallet from user data
            kraken_key = context.user_data.get('kraken_key')
            kraken_secret = context.user_data.get('kraken_secret')
            hl_key = context.user_data.get('hl_key')
            hl_secret = context.user_data.get('hl_secret')
            # hl_wallet is already assigned above

            if not all([kraken_key, kraken_secret, hl_key, hl_secret, hl_wallet_address]):
                 logger.error(f"Missing API key/secret/wallet data for user {user_id} during final save.")
                 await update.message.reply_text(
                    "❌ Something went wrong, missing some API key or wallet information. "
                    "Please start the API key setup again with /setkeys."
                 )
                 context.user_data.clear()
                 return ConversationHandler.END

            logger.info(f"All API keys and wallet collected for user {user_id}, proceeding to store them securely")

            # Encrypt and store Kraken API keys
            kraken_key_success = False
            hl_key_success = False
            hl_wallet_success = False

            if kraken_key and kraken_secret:
                try:
                    logger.info(f"Encrypting Kraken API keys for user {user_id}")
                    encrypted_kraken_key, iv_kraken_key = self.security.encrypt(kraken_key)
                    encrypted_kraken_secret, iv_kraken_secret = self.security.encrypt(kraken_secret)

                    kraken_key_success = self.db.store_api_key(
                        telegram_id=user_id,
                        exchange='kraken',
                        encrypted_key=encrypted_kraken_key,
                        encrypted_secret=encrypted_kraken_secret,
                        key_iv=iv_kraken_key,
                        secret_iv=iv_kraken_secret
                    )
                    logger.info(f"Kraken API keys storage {'successful' if kraken_key_success else 'failed'} for user {user_id}")
                except Exception as e:
                    logger.error(f"Error encrypting/storing Kraken API keys for user {user_id}: {str(e)}")

            # Encrypt and store Hyperliquid API keys
            if hl_key and hl_secret:
                try:
                    logger.info(f"Encrypting Hyperliquid API keys for user {user_id}")
                    encrypted_hl_key, iv_hl_key = self.security.encrypt(hl_key)
                    encrypted_hl_secret, iv_hl_secret = self.security.encrypt(hl_secret)

                    hl_key_success = self.db.store_api_key(
                        telegram_id=user_id,
                        exchange='hyperliquid',
                        encrypted_key=encrypted_hl_key,
                        encrypted_secret=encrypted_hl_secret,
                        key_iv=iv_hl_key,
                        secret_iv=iv_hl_secret
                    )
                    logger.info(f"Hyperliquid API keys storage {'successful' if hl_key_success else 'failed'} for user {user_id}")
                except Exception as e:
                    logger.error(f"Error encrypting/storing Hyperliquid API keys for user {user_id}: {str(e)}")

            # Store Hyperliquid Wallet Address in DB
            try:
                logger.info(f"Storing Hyperliquid wallet address for user {user_id}")
                # You might need to adapt this call based on your actual database method signature
                if hasattr(self.db, 'store_hyperliquid_wallet'):
                     hl_wallet_success = self.db.store_hyperliquid_wallet(
                        telegram_id=user_id,
                        wallet_address=hl_wallet_address
                     )
                elif hasattr(self.db, 'update_hyperliquid_wallet'): # Corrected check for the actual function name
                     hl_wallet_success = self.db.update_hyperliquid_wallet( # Corrected function call
                         telegram_id=user_id,
                         wallet_address=hl_wallet_address
                     )
                else:
                    logger.error(f"Database method for storing HL wallet not found for user {user_id}. Cannot store wallet.")
                    # Consider hl_wallet_success = False here?

                logger.info(f"Hyperliquid wallet storage {'successful' if hl_wallet_success else 'failed'} for user {user_id}")
            except Exception as e:
                logger.error(f"Error storing Hyperliquid wallet for user {user_id}: {str(e)}")

            # Clear sensitive data from context for security
            logger.info(f"Clearing sensitive data from context for user {user_id}")
            context.user_data.clear() # Clear all user_data at the end of the conversation

            # Show confirmation message
            if kraken_key_success and hl_key_success and hl_wallet_success:
                logger.info(f"All API keys and wallet successfully stored for user {user_id}")
                msg = await update.message.reply_text(
                    "🔐 All API keys and your Hyperliquid wallet address have been securely stored.\n\n"
                    "You can now use /tokens to select which tokens to trade,\n"
                    "and /start_bot to begin trading."
                )
            else:
                error_messages = []
                if not kraken_key_success: error_messages.append("Kraken keys")
                if not hl_key_success: error_messages.append("Hyperliquid keys")
                if not hl_wallet_success: error_messages.append("Hyperliquid wallet")
                logger.error(f"Failed to store some credentials for user {user_id}: {', '.join(error_messages)}")
                msg = await update.message.reply_text(
                    f"❌ There was an issue storing some of your details ({', '.join(error_messages)}). "
                    "Please try setting them again with /setkeys."
                )

            logger.info(f"API key setup completed for user {user_id}")
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Error in process_hl_wallet for user {update.effective_user.id}: {str(e)}")
            await update.message.reply_text(
                "❌ There was an error processing your API keys or wallet. Please try again with /setkeys"
            )
            context.user_data.clear() # Clear context on exception
            return ConversationHandler.END
        
    async def cmd_start_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /start_bot command"""
        user_id = update.effective_user.id
        user = self.db.get_user_by_telegram_id(user_id)
        
        if not user:
            await update.message.reply_text(
                "Please use /start to register first."
            )
            return ConversationHandler.END
            
        # Check for active subscription
        if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
            await update.message.reply_text(
                "You don't have an active subscription. Please subscribe first with /subscribe"
            )
            return ConversationHandler.END
            
        # Check if API keys are set
        kraken_key = self.db.get_api_key(user_id, 'kraken')
        hl_key = self.db.get_api_key(user_id, 'hyperliquid')
        
        if not kraken_key or not hl_key:
            await update.message.reply_text(
                "You need to set up your API keys first with /setkeys"
            )
            return ConversationHandler.END
            
        # Check if bot is already running
        bot_status = self.db.get_bot_status(user_id)
        if bot_status and bot_status.is_running:
            await update.message.reply_text(
                "Your bot is already running. You can check its status with /status"
            )
            return ConversationHandler.END
            
        # Get selected tokens
        selected_tokens = self.db.get_user_tokens(user_id)
        
        if not selected_tokens:
            await update.message.reply_text(
                "You haven't selected any tokens to trade. Please use /tokens to select trading pairs."
            )
            return ConversationHandler.END
            
        # Show confirmation
        keyboard = [
            [
                InlineKeyboardButton("✅ Start Bot", callback_data="confirm_start"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_start")
            ]
        ]
        
        tokens_text = ", ".join(selected_tokens)
            
        await update.message.reply_text(
            f"You're about to start the Abraxas Greenprint Funding Bot (Tier {user.subscription_tier})\n\n"
            f"*Trading tokens:* {tokens_text}\n\n"
            "The bot will use your API keys to place trades on Hyperliquid and Kraken.\n\n"
            "Are you sure you want to start the bot?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CONFIRM_START
        
    async def confirm_start_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle bot start confirmation"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "confirm_start":
            user_id = query.from_user.id
            user = self.db.get_user_by_telegram_id(user_id)
            
            # Decrypt API keys
            kraken_key = self.db.get_api_key(user_id, 'kraken')
            hl_key = self.db.get_api_key(user_id, 'hyperliquid')
            
            if not kraken_key or not hl_key:
                await query.edit_message_text(
                    "Failed to start bot: API keys not found. Please set them with /setkeys"
                )
                return ConversationHandler.END
                
            # Get selected tokens
            selected_tokens = self.db.get_user_tokens(user_id)
            
            if not selected_tokens:
                await query.edit_message_text(
                    "No tokens selected for trading. Please use /tokens to select tokens first."
                )
                return ConversationHandler.END
                
            # Decrypt the keys
            kraken_api_key = self.security.decrypt(kraken_key.encrypted_key, kraken_key.key_iv)
            kraken_api_secret = self.security.decrypt(kraken_key.encrypted_secret, kraken_key.secret_iv)
            hl_api_key = self.security.decrypt(hl_key.encrypted_key, hl_key.key_iv)
            hl_api_secret = self.security.decrypt(hl_key.encrypted_secret, hl_key.secret_iv)
            
            try:
                # Start the trading bot instance using the BotService method
                # The service now handles fetching keys/tokens/strategies from DB
                success = self.bot_service.start_bot_instance(str(user_id))

                # Original code below, commented out as service handles data retrieval
                # success = self.bot_service.start_bot(
                #     user_id=str(user_id),
                #     tier=user.subscription_tier,
                #     kraken_key=kraken_api_key,
                #     kraken_secret=kraken_api_secret,
                #     hl_key=hl_api_key,
                #     hl_secret=hl_api_secret,
                #     selected_tokens=selected_tokens  # Pass the selected tokens
                # )

                if success:
                    # Update bot status in database
                    self.db.update_bot_status(
                        telegram_id=user_id,
                        is_running=True,
                        start_time=datetime.now()
                    )
                    
                    # Format tokens for display
                    tokens_text = ", ".join(selected_tokens)
                    
                    # Use simpler message format to avoid Markdown parsing issues
                    await query.edit_message_text(
                        f"🚀 Abraxas Greenprint Funding Bot started successfully!\n\n"
                        f"Trading tokens: {tokens_text}\n\n"
                        "Your bot is now running and will automatically trade based on funding rates.\n\n"
                        "You can check its status anytime with /status\n"
                        "To stop the bot, use /stop_bot",
                        parse_mode=None  # Disable Markdown parsing
                    )
                else:
                    await query.edit_message_text(
                        "❌ Failed to start the bot. Please check your API keys and try again.",
                        parse_mode=None  # Disable Markdown parsing
                    )
            except Exception as e:
                logger.error(f"Error starting bot for user {user_id}: {str(e)}")
                # Remove Markdown for error messages to avoid parsing issues
                await query.edit_message_text(
                    f"❌ An error occurred while starting the bot: {str(e)}",
                    parse_mode=None  # Disable Markdown for error messages
                )
                
        elif query.data == "cancel_start":
            await query.edit_message_text("Bot start cancelled.")
            
        return ConversationHandler.END
        
    async def cmd_stop_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /stop_bot command"""
        user_id = update.effective_user.id
        
        # Check if bot is running
        bot_status = self.db.get_bot_status(user_id)
        if not bot_status or not bot_status.is_running:
            await update.message.reply_text(
                "Your bot is not currently running. You can start it with /start_bot"
            )
            return ConversationHandler.END
            
        # Show confirmation
        keyboard = [
            [
                InlineKeyboardButton("⚠️ Stop Bot", callback_data="confirm_stop"),
                InlineKeyboardButton("Cancel", callback_data="cancel_stop")
            ]
        ]
        
        await update.message.reply_text(
            "⚠️ Are you sure you want to stop your trading bot?\n\n"
            "This will close all open orders and exit active positions.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return CONFIRM_STOP
        
    async def confirm_stop_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle bot stop confirmation"""
        query = update.callback_query
        await query.answer()
        
        if query.data == "confirm_stop":
            user_id = query.from_user.id
            
            try:
                # Stop the trading bot
                success = self.bot_service.stop_bot(str(user_id))
                
                if success:
                    # Update bot status in database
                    self.db.update_bot_status(
                        telegram_id=user_id,
                        is_running=False,
                        stop_time=datetime.now()
                    )
                    
                    await query.edit_message_text(
                        "✅ Bot stopped successfully.\n\n"
                        "All positions have been closed and trading has stopped."
                    )
                else:
                    await query.edit_message_text(
                        "❌ Failed to stop the bot. Please try again or contact support."
                    )
            except Exception as e:
                logger.error(f"Error stopping bot for user {user_id}: {str(e)}")
                await query.edit_message_text(
                    f"❌ An error occurred while stopping the bot: {str(e)}"
                )
                
        elif query.data == "cancel_stop":
            await query.edit_message_text("Bot will continue running.")
            
        return ConversationHandler.END
        
    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /status command"""
        user_id = update.effective_user.id
        user = self.db.get_user_by_telegram_id(user_id)
        
        if not user:
            await update.message.reply_text(
                "Please use /start to register first."
            )
            return
            
        # Get subscription info
        subscription_status = "No active subscription"
        if user.subscription_tier and user.subscription_expiry:
            if user.subscription_expiry > datetime.now():
                days_left = (user.subscription_expiry - datetime.now()).days
                subscription_status = f"Tier {user.subscription_tier} - {days_left} days left"
            else:
                subscription_status = f"Tier {user.subscription_tier} - Expired"
                
        # Get selected tokens
        selected_tokens = self.db.get_user_tokens(user_id)
        token_status = ", ".join(selected_tokens) if selected_tokens else "None"
                
        # Get bot status
        bot_status = self.db.get_bot_status(user_id)
        status_text = "Not running"
        uptime = "N/A"
        
        if bot_status and bot_status.is_running:
            status_text = "🟢 Running"
            if bot_status.start_time:
                uptime_seconds = (datetime.now() - bot_status.start_time).total_seconds()
                hours, remainder = divmod(uptime_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime = f"{int(hours)}h {int(minutes)}m"
                
        # Get actual bot positions and balances from service if running
        positions_text = "No active positions"
        balances_text = "No balance data available"
        funding_rates_text = "No funding rate data available"
        strategies_text = "No strategy data available"

        if bot_status and bot_status.is_running:
            bot_info = self.bot_service.get_bot_status(str(user_id))
            if bot_info:
                # Format positions
                if 'positions' in bot_info and bot_info['positions']:
                    positions_text = ""
                    for asset, pos in bot_info['positions'].items():
                        positions_text += f"\n{asset}: HL={pos['hl_position']}, Kraken={pos['kraken_position']}"
                    
                # Format balances
                if 'balances' in bot_info and bot_info['balances']:
                    balances_text = ""
                    for exchange, balance in bot_info['balances'].items():
                        if isinstance(balance, (int, float)):
                            balances_text += f"\n{exchange}: ${balance:.2f}"
                        else:
                            balances_text += f"\n{exchange}: {balance}"

                # Format funding rates
                if 'funding_rates' in bot_info and bot_info['funding_rates']:
                    funding_rates_text = ""
                    for asset, rate in bot_info['funding_rates'].items():
                        if isinstance(rate, (int, float)):
                            funding_rates_text += f"\n{asset}: {rate:.4f}%"
                        else:
                            funding_rates_text += f"\n{asset}: {rate}"

                # Format strategies
                if 'strategies' in bot_info:
                    strategies_text = f"Entry: {bot_info['strategies'].get('entry_strategy', 'default')}, Exit: {bot_info['strategies'].get('exit_strategy', 'default')}"

        # Format status message
        status_message = (
            "📊 *Abraxas Greenprint Status*\n\n"
            f"*Subscription:* {subscription_status}\n"
            f"*Selected Tokens:* {token_status}\n"
            f"*Bot Status:* {status_text}\n"
            f"*Uptime:* {uptime}\n\n"
            f"*Strategies:*\n{strategies_text}\n\n"
            f"*Current Funding Rates:*\n{funding_rates_text}\n\n"
            f"*Exchange Balances:*\n{balances_text}\n\n"
            f"*Active Positions:*\n{positions_text}\n\n"
            f"*Commands:*\n"
            f"• /tokens - Change token selections\n"
            f"• /strategies - Configure entry/exit strategies\n"
            f"• /start_bot - Start trading\n"
            f"• /stop_bot - Stop trading\n"
            f"• /status - Refresh this status"
        )
        
        await update.message.reply_text(status_message, parse_mode='Markdown')
        
    async def cancel_conversation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel current conversation"""
        await update.message.reply_text(
            "Operation cancelled."
        )
        return ConversationHandler.END
        
    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle unknown commands"""
        command = update.message.text.split()[0]  # Get the command part
        
        # List all recognized commands
        valid_commands = ["/start", "/help", "/pairs", "/subscribe", "/tokens", 
                         "/setkeys", "/status", "/start_bot", "/stop_bot", "/cancel"]
        
        self.logger.info(f"Received command: {command}")
        
        if command in valid_commands:
            # It's a valid command but wasn't properly handled
            self.logger.error(f"Command {command} not properly processed despite being valid")
            await update.message.reply_text(
                f"Command {command} was recognized but could not be processed. "
                "Please try again or use /start to restart the bot."
            )
        else:
            # Truly unknown command
            self.logger.warning(f"Unknown command received: {command}")
            await update.message.reply_text(
                f"Sorry, I don't understand the command '{command}'. "
                "Use /help to see available commands."
            )
        
    async def verify_payment_task(self, user_id, payment_id):
        """Background task to verify payment"""
        try:
            self.logger.info(f"Starting payment verification for user {user_id}, payment {payment_id}")
            
            # Get transaction data
            transaction_data = self.db.get_transaction(payment_id)
            if not transaction_data:
                self.logger.error(f"Transaction {payment_id} not found in database")
                return
            
            # Check if payment is already confirmed
            if transaction_data.payment_status == 'completed':
                self.logger.info(f"Payment {payment_id} already confirmed")
                return
            
            # Get payment data and tier
            payment_data = json.loads(transaction_data.payment_data) if transaction_data.payment_data else {}
            tier = transaction_data.tier
            self.logger.info(f"Processing payment for tier {tier}")
            
            # First, quickly check if webhook has already confirmed the payment
            transaction_data = self.db.get_transaction(payment_id)
            if transaction_data.payment_status == 'completed':
                # Payment already confirmed via webhook!
                self.logger.info(f"Payment {payment_id} already confirmed via webhook")
                await self._send_confirmation_and_setup_keys(user_id, payment_id, tier)
                return
                
            # Wait for payment confirmation via webhook with patience
            # More attempts with longer increasing intervals
            wait_times = [10, 15, 20, 30, 45]  # seconds
            
            for attempt, wait_time in enumerate(wait_times):
                self.logger.info(f"Verifying payment attempt {attempt+1}/{len(wait_times)}")
                
                # Check payment status in our database (may have been updated by webhook)
                transaction_data = self.db.get_transaction(payment_id)
                
                if transaction_data.payment_status == 'completed':
                    # Payment confirmed via webhook!
                    self.logger.info(f"Payment {payment_id} confirmed via webhook on attempt {attempt+1}")

                    # Double check the tier
                    if transaction_data.tier != tier:
                        self.logger.warning(f"Tier mismatch! Expected {tier}, got {transaction_data.tier}")
                        tier = transaction_data.tier

                    await self._send_confirmation_and_setup_keys(user_id, payment_id, tier)
                    return
                
                # Try direct API verification every other attempt
                if attempt % 2 == 1:
                    if self.payment.verify_payment(payment_id):
                        self.logger.info(f"Payment {payment_id} confirmed via API on attempt {attempt+1}")
                        
                        # Update transaction status
                        self.db.update_transaction_status(payment_id, 'completed')
                        
                        # Verify the tier one more time
                        transaction_data = self.db.get_transaction(payment_id)
                        if transaction_data.tier != tier:
                            self.logger.warning(f"Tier mismatch! Expected {tier}, got {transaction_data.tier}")
                            tier = transaction_data.tier

                        await self._send_confirmation_and_setup_keys(user_id, payment_id, tier)
                        return
                
                # Tell the user we're still working on it if it's taking a while
                if attempt == 2:  # Send after ~45 seconds
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text="We're still verifying your payment... This may take a moment. We'll notify you as soon as it's confirmed."
                    )
                
                # Wait before next attempt
                await asyncio.sleep(wait_time)
            
            # If we get here, try one final direct verification
            if self.payment.verify_payment(payment_id):
                self.logger.info(f"Payment {payment_id} confirmed via API on final attempt")
                
                # Update transaction status
                self.db.update_transaction_status(payment_id, 'completed')
                
                # Final tier verification
                transaction_data = self.db.get_transaction(payment_id)
                if transaction_data.tier != tier:
                    self.logger.warning(f"Tier mismatch! Expected {tier}, got {transaction_data.tier}")
                    tier = transaction_data.tier

                await self._send_confirmation_and_setup_keys(user_id, payment_id, tier)
            else:
                # Payment not confirmed after all attempts
                self.logger.warning(f"Payment {payment_id} not confirmed after {len(wait_times)} verification attempts")
                
                # Send notification to user
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text="⏳ We haven't been able to confirm your payment yet.\n\n"
                         "If you've completed the payment, this might take a few more minutes depending on blockchain confirmation times.\n\n"
                         "You'll receive a notification once it's confirmed, or you can check your status with /status."
                )
        
        except Exception as e:
            self.logger.error(f"Error in payment verification: {str(e)}")
            # Send error notification to user
            try:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text="❌ There was an error verifying your payment. Please contact support."
                )
            except:
                pass
                
    async def _send_confirmation_and_setup_keys(self, user_id, payment_id, tier):
        """Helper method to send payment confirmation and start API key setup"""
        try:
            logger.info(f"Processing payment confirmation for user {user_id}, payment {payment_id}, tier {tier}")

            # Update subscription
            from datetime import datetime, timedelta
            expiry_date = datetime.now() + timedelta(days=30)
            logger.info(f"Updating user {user_id} subscription to tier {tier} with expiry {expiry_date}")

            # Update subscription and verify the update
            success = self.db.update_user_subscription(
                telegram_id=user_id,
                tier=tier,
                expiry=expiry_date
            )

            if not success:
                logger.error(f"Failed to update subscription for user {user_id} to tier {tier}")
                raise Exception("Failed to update subscription in database")

            # Verify the update with multiple retries
            max_retries = 3
            retry_delay = 1  # seconds

            for attempt in range(max_retries):
                user = self.db.get_user_by_telegram_id(user_id)
                if user and user.subscription_tier == tier:
                    logger.info(f"Subscription verified for user {user_id} on attempt {attempt + 1}")
                    break

                if attempt < max_retries - 1:
                    logger.warning(f"Subscription verification failed on attempt {attempt + 1}, retrying...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Subscription verification failed after {max_retries} attempts")
                    raise Exception("Subscription verification failed")

            logger.info(f"Successfully updated and verified subscription for user {user_id} to tier {tier}")

            # Send confirmation to user with tier-specific message
            token_limit = "1 token" if tier == 1 else "2 tokens" if tier == 2 else "all available tokens"
            logger.info(f"Sending confirmation message to user {user_id} for tier {tier} with token limit {token_limit}")

            try:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text=f"✅ *Payment Confirmed Successfully!* Your Tier {tier} subscription is now active.\n\n"
                          f"Your subscription allows you to select {token_limit} for trading.\n\n"
                          f"🔐 *Next Step: Select your trading tokens*\n\n"
                          f"Please use the button below to select your tokens:",
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Select Tokens", callback_data="guide_tokens")]
                    ])
                )
                logger.info(f"Sent confirmation message to user {user_id}")
            except Exception as msg_error:
                logger.error(f"Failed to send confirmation message: {str(msg_error)}")
                logger.error(f"Error in _send_confirmation_and_setup_keys for user {user_id}: {str(msg_error)}")
                logger.error(f"Payment ID: {payment_id}, Tier: {tier}")
                # Try sending a simpler message without markdown or buttons
                try:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=f"✅ Your payment has been confirmed! Your Tier {tier} subscription is now active.\n\n"
                              "Please use /tokens to select your trading tokens."
                        )
                except Exception as fallback_error:
                    logger.error(f"Failed to send fallback message to user {user_id}: {str(fallback_error)}")

        except Exception as e:
            logger.error(f"Error in _send_confirmation_and_setup_keys for user {user_id}: {str(e)}")
            logger.error(f"Payment ID: {payment_id}, Tier: {tier}")
            # Send a fallback message
            try:
                await self.application.bot.send_message(
                    chat_id=user_id,
                    text=f"✅ Your payment has been confirmed! Your Tier {tier} subscription is now active.\n\n"
                          "Please use /tokens to select your trading tokens."
                )
            except Exception as fallback_error:
                logger.error(f"Failed to send fallback message to user {user_id}: {str(fallback_error)}")
        
    async def guide_tokens_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle guided token setup button from webhook notification"""
        query = update.callback_query
        user_id = update.effective_user.id

        logger.info(f"Starting token selection for user {user_id}")
        await query.answer()
        
        # End any existing conversations to avoid state conflicts
        context.user_data.clear()
        logger.info(f"Cleared user context for {user_id}")

        try:
            # Get user from database with retries
            max_retries = 3
            retry_delay = 1  # seconds
            user = None

            for attempt in range(max_retries):
                user = self.db.get_user_by_telegram_id(user_id)
                if user and user.subscription_tier:
                    logger.info(f"Found user {user_id} with tier {user.subscription_tier} on attempt {attempt + 1}")
                    break

                if attempt < max_retries - 1:
                    logger.warning(f"User or tier not found on attempt {attempt + 1}, retrying...")
                    await asyncio.sleep(retry_delay)

            if not user:
                logger.warning(f"User {user_id} not found in database after {max_retries} attempts")
                await query.edit_message_text("Please use /start to register first.")
                return ConversationHandler.END
            
            logger.info(f"Found user {user_id} with tier {user.subscription_tier}")

            # Check if user has an active subscription
            if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
                logger.warning(f"User {user_id} has no active subscription")
                await query.edit_message_text("You don't have an active subscription. Please subscribe first with /subscribe")
                return ConversationHandler.END
            
            # Get current token selections
            current_tokens = self.db.get_user_tokens(user_id)
            max_tokens = self.token_limits[user.subscription_tier]
            logger.info(f"User {user_id} has {len(current_tokens)}/{max_tokens} tokens selected")
        
            # Refresh available pairs
            self.available_pairs = get_active_trading_pairs()
            logger.info(f"Found {len(self.available_pairs)} available trading pairs")
        
            # Create token selection UI
            token_buttons = []
            for token in self.available_pairs:
                if token in current_tokens:
                    token_buttons.append([
                        InlineKeyboardButton(f"✅ {token}", callback_data=f"toggle_{token}")
                    ])
                else:
                    token_buttons.append([
                        InlineKeyboardButton(token, callback_data=f"toggle_{token}")
                    ])
        
            # Add the Confirm Token Selection button
            token_buttons.append([
                InlineKeyboardButton("Confirm Token Selection", callback_data="save_tokens")
            ])
        
            # Store current tokens in context
            context.user_data['current_tokens'] = current_tokens.copy()
            context.user_data['in_token_selection'] = True
            logger.info(f"Stored {len(current_tokens)} tokens in context for user {user_id}")
        
            # Format current selections text
            current_text = ", ".join(current_tokens) if current_tokens else "None"

            # Create tier-specific message
            token_limit_text = "1 token" if user.subscription_tier == 1 else "2 tokens" if user.subscription_tier == 2 else "all available tokens"
        
            message_text = (
                f"*Token Selection*\n\n"
                f"Your subscription (Tier {user.subscription_tier}) allows you to select {token_limit_text}.\n\n"
                f"*Current selections:* {current_text}\n"
                f"({len(current_tokens)}/{max_tokens})\n\n"
                f"Tap on tokens to select or deselect them. There are {len(self.available_pairs)} tokens available:"
            )
        
            logger.info(f"Sending token selection interface to user {user_id}")
            try:
                # Edit the existing message with the new content and buttons
                await query.edit_message_text(
                    message_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(token_buttons)
                )
                logger.info(f"Successfully edited message for user {user_id}")
            except Exception as edit_error:
                logger.error(f"Failed to edit message for user {user_id}: {str(edit_error)}")
                # If editing fails, send a new message
                await query.message.reply_text(
                    message_text,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(token_buttons)
                )
                logger.info(f"Sent new message to user {user_id} after edit failed")
        
            return CHOOSING_TOKENS
        
        except Exception as e:
            logger.error(f"Unexpected error in guide_tokens_callback for user {user_id}: {str(e)}")
            logger.error(f"Error details: {type(e).__name__}: {str(e)}")
            try:
                await query.edit_message_text(
                    "❌ An error occurred while setting up token selection. Please try again."
                )
            except:
                pass
            return ConversationHandler.END

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle errors in the bot"""
        try:
            error = context.error
            logger.error(f"Exception while handling an update: {error}")
            logger.error(f"Error type: {type(error).__name__}")
            logger.error(f"Error details: {str(error)}")
            
            if isinstance(error, Conflict):
                logger.error("Bot conflict detected. Another instance might be running.")
                # Try to gracefully shut down
                await self.application.stop()
                # Exit the program
                sys.exit(1)
            else:
                # Log other errors but don't crash
                logger.error(f"Non-critical error in update: {error}")
                logger.error(f"Update details: {update}")
                logger.error(f"Context details: {context}")
        except Exception as e:
            logger.error(f"Error in error handler: {str(e)}")
    
    async def delete_webhook(self):
        """Asynchronously delete any existing webhook."""
        try:
            await self.application.bot.delete_webhook()
            logger.info("Successfully deleted webhook")
        except TelegramError as e:
            logger.warning(f"Error deleting webhook: {e}")
        except Exception as e:
            logger.error(f"Unexpected error deleting webhook: {e}")

    def run(self):
        """Start the bot in polling mode."""
        try:
            # Delete any existing webhook
            asyncio.get_event_loop().run_until_complete(self.delete_webhook())
            
            # Start polling
            logger.info("Starting bot in polling mode...")
            self.application.run_polling()
            
        except Exception as e:
            if isinstance(e, Conflict):
                logger.error(f"Bot conflict detected: {e}")
                logger.error("Another instance might be running. Use cleanup_bot.py to terminate existing instances.")
            else:
                logger.error(f"Failed to start polling: {str(e)}")
            sys.exit(1)

    async def guide_keys_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle guided API key setup button"""
        query = update.callback_query
        user_id = query.from_user.id

        logger.info(f"Starting API key setup for user {user_id}")
        await query.answer()
        
        # End any existing conversations to avoid state conflicts
        context.user_data.clear()
        logger.info(f"Cleared user context for {user_id}")
        
        try:
            # Get user from database
            user = self.db.get_user_by_telegram_id(user_id)
            if not user:
                logger.warning(f"User {user_id} not found in database")
                await query.edit_message_text("Please use /start to register first.")
                return ConversationHandler.END
                
            logger.info(f"Found user {user_id} with tier {user.subscription_tier}")

            # Check if user has an active subscription
            if not user.subscription_tier or not user.subscription_expiry or user.subscription_expiry < datetime.now():
                logger.warning(f"User {user_id} has no active subscription")
                await query.edit_message_text("You don't have an active subscription. Please subscribe first with /subscribe")
                return ConversationHandler.END
            
            # Start API key setup
            await query.edit_message_text(
                "🔑 Let's set up your API keys.\n\n"
                "⚠️ *IMPORTANT*: Never share your API keys with anyone else!\n\n"
                "First, please enter your Kraken API Key:",
                parse_mode='Markdown'
            )
            
            return AWAITING_KRAKEN_KEY

        except Exception as e:
            logger.error(f"Unexpected error in guide_keys_callback for user {user_id}: {str(e)}")
            logger.error(f"Error details: {type(e).__name__}: {str(e)}")
            try:
                await query.edit_message_text(
                    "❌ An error occurred while setting up API keys. Please try again."
                )
            except:
                pass
            return ConversationHandler.END
    
    async def handle_webhook(self, request):
        """Handle incoming webhook updates."""
        try:
            # Get the update from the request
            data = await request.json()
            logger.info(f"Received webhook update: {data}")

            # Process the update
            update = Update.de_json(data, self.application.bot)
            await self.application.process_update(update)

            return web.Response(text="OK")
        except Exception as e:
            logger.error(f"Error processing webhook update: {str(e)}", exc_info=True)
            return web.Response(text="Error", status=500)

    async def setup_webhook(self, webhook_url):
        """Set up webhook for the bot.

        Args:
            webhook_url: The URL to receive webhook updates

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Delete any existing webhook
            await self.delete_webhook()

            # Set the new webhook
            await self.application.bot.set_webhook(url=webhook_url)
            logger.info(f"Webhook set to {webhook_url}")
            return True

        except Exception as e:
            logger.error(f"Failed to set webhook: {e}")
            return False

if __name__ == "__main__":
    # Set up logging first
    logging.basicConfig(
        filename='/opt/crypto-arb-bot/logs/bot.log',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)

    try:
        # Create bot instance
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not bot_token:
            logger.error("TELEGRAM_BOT_TOKEN environment variable not set")
            sys.exit(1)

        # Create bot instance
        bot = AbraxasGreenprintBot(bot_token)
        logger.info("Bot instance created successfully")

        # Register error handler
        bot.application.add_error_handler(bot.error_handler)
        logger.info("Error handler registered")

        # Check if running under Gunicorn
        if 'gunicorn' in os.environ.get('SERVER_SOFTWARE', ''):
            # Webhook mode
            webhook_url = os.getenv('WEBHOOK_URL')
            if not webhook_url:
                logger.error("WEBHOOK_URL environment variable not set")
                sys.exit(1)

            try:
                # Set up webhook
                bot.application.bot.set_webhook(webhook_url)
                logger.info(f"Bot started in webhook mode at {webhook_url}")

                # Create web app
                app = web.Application()
                app.router.add_post(f"/{bot_token}", bot.handle_webhook)

                # Export for Gunicorn
                web_app = app
            except Exception as e:
                logger.error(f"Failed to set up webhook: {str(e)}", exc_info=True)
                sys.exit(1)
        else:
            # Polling mode
            try:
                logger.info("Bot started in polling mode")
                bot.run()
            except Exception as e:
                logger.error(f"Failed to start polling: {str(e)}", exc_info=True)
                sys.exit(1)
    except Exception as e:
        logger.error(f"Critical error during bot startup: {str(e)}", exc_info=True)
        sys.exit(1)