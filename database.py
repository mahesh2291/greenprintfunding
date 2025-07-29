#!/usr/bin/env python3
"""
Database Module for the Abraxas Greenprint Funding Bot
-------------------------------------------
Manages database interactions
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, DateTime, ForeignKey, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session

# Configure logging
logger = logging.getLogger(__name__)

# Get database URL from environment or use SQLite as default
DB_URL = os.getenv('DATABASE_URL', 'sqlite:///abraxas_bot.db')

# Create SQLAlchemy base
Base = declarative_base()

# Define the ORM models
class User(Base):
    """User model for storing user information"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    username = Column(String, nullable=True)
    email = Column(String, nullable=True)  # Added email field
    subscription_tier = Column(Integer, nullable=True)
    subscription_expiry = Column(DateTime, nullable=True)
    selected_tokens = Column(String, nullable=True)  # Comma-separated list of selected tokens
    entry_strategy = Column(String, nullable=True, default="default")  # New field for entry strategy
    exit_strategy = Column(String, nullable=True, default="default")   # New field for exit strategy
    hyperliquid_wallet_address = Column(String, nullable=True) # Added Hyperliquid wallet address field
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationships
    api_keys = relationship("APIKey", back_populates="user", cascade="all, delete-orphan")
    bot_status = relationship("BotStatus", back_populates="user", uselist=False, cascade="all, delete-orphan")
    transactions = relationship("Transaction", back_populates="user", cascade="all, delete-orphan")
    token_selections = relationship("TokenSelection", back_populates="user", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<User(telegram_id={self.telegram_id}, tier={self.subscription_tier})>"

class TokenSelection(Base):
    """Model for storing user's selected trading tokens"""
    __tablename__ = 'token_selections'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    token = Column(String, nullable=False)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    user = relationship("User", back_populates="token_selections")
    
    def __repr__(self):
        return f"<TokenSelection(user_id={self.user_id}, token={self.token}, active={self.active})>"
        
class APIKey(Base):
    """API Key model for storing encrypted API keys"""
    __tablename__ = 'api_keys'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    exchange = Column(String, nullable=False)
    encrypted_key = Column(LargeBinary, nullable=False)
    encrypted_secret = Column(LargeBinary, nullable=False)
    key_iv = Column(LargeBinary, nullable=False)
    secret_iv = Column(LargeBinary, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    user = relationship("User", back_populates="api_keys")
    
    def __repr__(self):
        return f"<APIKey(user_id={self.user_id}, exchange={self.exchange})>"
        
class BotStatus(Base):
    """Bot Status model for tracking bot status"""
    __tablename__ = 'bot_status'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, unique=True)
    is_running = Column(Boolean, default=False)
    start_time = Column(DateTime, nullable=True)
    stop_time = Column(DateTime, nullable=True)
    error_count = Column(Integer, default=0)
    last_error = Column(String, nullable=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    user = relationship("User", back_populates="bot_status")
    
    def __repr__(self):
        return f"<BotStatus(user_id={self.user_id}, is_running={self.is_running})>"
        
class Transaction(Base):
    """Transaction model for tracking payments"""
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, default="USD")
    tier = Column(Integer, nullable=False)
    transaction_id = Column(String, unique=True, nullable=False)
    payment_status = Column(String, default="pending")
    payment_data = Column(String, nullable=True)  # JSON string with payment details
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Relationship
    user = relationship("User", back_populates="transactions")
    
    def __repr__(self):
        return f"<Transaction(user_id={self.user_id}, amount={self.amount}, status={self.payment_status})>"
        
class Database:
    """Database manager class"""
    
    def __init__(self):
        """Initialize database connection"""
        self.engine = create_engine(DB_URL)
        Base.metadata.create_all(self.engine)
        session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(session_factory)

        # Force SQLAlchemy to refresh its metadata
        Base.metadata.reflect(bind=self.engine)
        
    def get_user_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        """Get user by Telegram ID"""
        with self.Session() as session:
            return session.query(User).filter_by(telegram_id=telegram_id).first()
            
    def create_user(self, telegram_id: int, username: str = None) -> User:
        """Create a new user"""
        with self.Session() as session:
            # Check if user already exists
            existing_user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if existing_user:
                return existing_user
                
            # Create new user
            new_user = User(telegram_id=telegram_id, username=username)
            session.add(new_user)
            session.commit()
            return new_user
            
    def update_user_subscription(self, telegram_id: int, tier: int, expiry: datetime) -> bool:
        """Update user subscription"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            user.subscription_tier = tier
            user.subscription_expiry = expiry
            session.commit()
            return True
            
    def update_user_strategies(self, telegram_id: int, entry_strategy: str = None, exit_strategy: str = None) -> bool:
        """Update user's entry and exit strategies"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
            
            if entry_strategy:
                user.entry_strategy = entry_strategy
            if exit_strategy:
                user.exit_strategy = exit_strategy
                
            session.commit()
            return True
            
    def get_user_strategies(self, telegram_id: int) -> Dict[str, str]:
        """Get user's entry and exit strategies"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return {"entry_strategy": "default", "exit_strategy": "default"}
                
            return {
                "entry_strategy": user.entry_strategy or "default",
                "exit_strategy": user.exit_strategy or "default"
            }
            
    def get_user_tokens(self, telegram_id: int) -> List[str]:
        """Get user's selected tokens"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return []
                
            token_selections = session.query(TokenSelection).filter_by(
                user_id=user.id, active=True
            ).all()
            
            return [ts.token for ts in token_selections]
            
    def update_user_tokens(self, telegram_id: int, tokens: List[str]) -> bool:
        """Update user's selected tokens"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            # First deactivate all existing tokens
            session.query(TokenSelection).filter_by(user_id=user.id).update(
                {"active": False}
            )
            
            # Now add or reactivate the selected tokens
            for token in tokens:
                existing = session.query(TokenSelection).filter_by(
                    user_id=user.id, token=token
                ).first()
                
                if existing:
                    existing.active = True
                    existing.updated_at = datetime.now()
                else:
                    new_token = TokenSelection(
                        user_id=user.id,
                        token=token,
                        active=True
                    )
                    session.add(new_token)
                    
            # Also update the comma-separated list for quick access
            user.selected_tokens = ",".join(tokens)
            
            session.commit()
            return True
            
    def store_api_key(self, telegram_id: int, exchange: str, encrypted_key: bytes, encrypted_secret: bytes, 
                     key_iv: bytes, secret_iv: bytes) -> bool:
        """Store API keys for a user"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            # Check if keys for this exchange already exist
            existing_key = session.query(APIKey).filter_by(user_id=user.id, exchange=exchange).first()
            
            if existing_key:
                # Update existing keys
                existing_key.encrypted_key = encrypted_key
                existing_key.encrypted_secret = encrypted_secret
                existing_key.key_iv = key_iv
                existing_key.secret_iv = secret_iv
            else:
                # Create new keys
                new_key = APIKey(
                    user_id=user.id,
                    exchange=exchange,
                    encrypted_key=encrypted_key,
                    encrypted_secret=encrypted_secret,
                    key_iv=key_iv,
                    secret_iv=secret_iv
                )
                session.add(new_key)
                
            session.commit()
            return True
            
    def get_api_key(self, telegram_id: int, exchange: str) -> Optional[APIKey]:
        """Get API keys for a user"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return None
                
            return session.query(APIKey).filter_by(user_id=user.id, exchange=exchange).first()
            
    def update_bot_status(self, telegram_id: int, is_running: bool, 
                         start_time: datetime = None, stop_time: datetime = None) -> bool:
        """Update bot status"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            # Get or create bot status
            status = session.query(BotStatus).filter_by(user_id=user.id).first()
            if not status:
                status = BotStatus(user_id=user.id)
                session.add(status)
                
            status.is_running = is_running
            if start_time:
                status.start_time = start_time
            if stop_time:
                status.stop_time = stop_time
                
            session.commit()
            return True
            
    def get_bot_status(self, telegram_id: int) -> Optional[BotStatus]:
        """Get bot status"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return None
                
            return session.query(BotStatus).filter_by(user_id=user.id).first()
            
    def record_bot_error(self, telegram_id: int, error_message: str) -> bool:
        """Record bot error"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            status = session.query(BotStatus).filter_by(user_id=user.id).first()
            if not status:
                status = BotStatus(user_id=user.id)
                session.add(status)
                
            status.error_count += 1
            status.last_error = error_message
            session.commit()
            return True
            
    def create_transaction(self, user_id: int, amount: float, tier: int, transaction_id: str, 
                          currency: str = "USD", payment_data: str = None) -> Optional[Transaction]:
        """Create a new transaction"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=user_id).first()
            if not user:
                return None
                
            transaction = Transaction(
                user_id=user.id,
                amount=amount,
                currency=currency,
                tier=tier,
                transaction_id=transaction_id,
                payment_data=payment_data
            )
            session.add(transaction)
            session.commit()
            return transaction
            
    def get_transaction(self, transaction_id: str) -> Optional[Transaction]:
        """Get transaction by ID"""
        with self.Session() as session:
            return session.query(Transaction).filter_by(transaction_id=transaction_id).first()
        
    def get_transaction_with_user(self, transaction_id: str) -> Optional[Dict]:
        """
        Get transaction with minimal required user information
        Returns only the essential fields needed for payment verification
        """
        with self.Session() as session:
            transaction = session.query(Transaction).filter_by(transaction_id=transaction_id).first()
            
            if not transaction:
                return None
                
            # Get user's telegram_id
            user = session.query(User).filter_by(id=transaction.user_id).first()
            if not user:
                return None
                
            # Return only the essential fields needed for payment verification
            return {
                'tier': transaction.tier,
                'user_id': user.telegram_id,
                'status': transaction.payment_status
            }
            
    def update_transaction_status(self, transaction_id: str, status: str) -> bool:
        """Update transaction status"""
        with self.Session() as session:
            transaction = session.query(Transaction).filter_by(transaction_id=transaction_id).first()
            if not transaction:
                return False
                
            transaction.payment_status = status
            session.commit()
            return True
            
    def update_user_email(self, telegram_id: int, email: str) -> bool:
        """Update user's email address"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
                
            user.email = email
            session.commit()
            return True
            
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email address"""
        with self.Session() as session:
            return session.query(User).filter_by(email=email).first()
            
    # Convenience method to get telegram_id directly from email
    def get_telegram_id_by_email(self, email: str) -> Optional[int]:
        """Get a user's Telegram ID by their email address"""
        with self.Session() as session:
            user = session.query(User).filter_by(email=email).first()
            return user.telegram_id if user else None

    def get_hyperliquid_wallet(self, telegram_id: int) -> Optional[str]:
        """Get user's Hyperliquid wallet address"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if user:
                return user.hyperliquid_wallet_address
            return None

    def update_hyperliquid_wallet(self, telegram_id: int, wallet_address: str) -> bool:
        """Update user's Hyperliquid wallet address"""
        with self.Session() as session:
            user = session.query(User).filter_by(telegram_id=telegram_id).first()
            if not user:
                return False
            user.hyperliquid_wallet_address = wallet_address
            session.commit()
            return True
