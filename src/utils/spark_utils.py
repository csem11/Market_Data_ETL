"""
Spark utilities for session management and configuration
Centralized Spark session management with proper configuration
"""

import os
from typing import Optional, Dict, Any
from contextlib import contextmanager
from pyspark.sql import SparkSession


class SparkSessionManager:
    """Manages Spark session lifecycle with proper configuration"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_session(cls, app_name: str = "MarketDataETL", 
                   master: str = None,
                   config: Optional[Dict[str, str]] = None) -> SparkSession:
        """
        Get or create a Spark session with proper configuration
        
        Args:
            app_name: Application name for Spark UI
            master: Spark master URL (auto-detected if None)
            config: Additional configuration dictionary
            
        Returns:
            Configured SparkSession
        """
        if cls._instance is None or cls._instance._sc._jsc is None:
            # Auto-detect master if not provided
            if master is None:
                master = cls._get_optimal_master()
            
            builder = SparkSession.builder.appName(app_name).master(master)
            
            # Get environment-specific configuration
            spark_config = cls._get_environment_config()
            
            # Merge with provided config
            if config:
                spark_config.update(config)
            
            # Apply all configurations
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            cls._instance = builder.getOrCreate()
        
        return cls._instance
    
    @classmethod
    def _get_optimal_master(cls) -> str:
        """Auto-detect the best master URL based on environment"""
        if os.getenv("SPARK_MASTER"):
            return os.getenv("SPARK_MASTER")
        elif os.getenv("SPARK_ENV") == "production":
            return "yarn"
        elif os.getenv("SPARK_ENV") == "cluster":
            return "spark://master:7077"
        else:
            return "local[*]"
    
    @classmethod
    def _get_environment_config(cls) -> Dict[str, str]:
        """Get environment-specific Spark configuration"""
        base_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000"
        }
        
        env = os.getenv("SPARK_ENV", "development")
        
        if env == "production":
            base_config.update({
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "20",
                "spark.dynamicAllocation.initialExecutors": "5",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.adaptive.localShuffleReader.enabled": "true"
            })
        elif env == "development":
            base_config.update({
                "spark.sql.adaptive.enabled": "false",  # Easier debugging
                "spark.sql.adaptive.coalescePartitions.enabled": "false",
                "spark.sql.adaptive.skewJoin.enabled": "false"
            })
        
        return base_config
    
    @classmethod
    def stop_session(cls):
        """Stop the current Spark session"""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
    
    @classmethod
    def reset_session(cls):
        """Reset the session (useful for testing)"""
        cls.stop_session()
        cls._instance = None


@contextmanager
def spark_session(app_name: str = "MarketDataETL", 
                 master: str = None,
                 config: Optional[Dict[str, str]] = None):
    """
    Context manager for Spark session lifecycle management
    
    Usage:
        with spark_session("MyApp") as spark:
            df = spark.read.csv("data.csv")
            # Session automatically cleaned up
    """
    session = None
    try:
        session = SparkSessionManager.get_session(app_name, master, config)
        yield session
    finally:
        if session is not None:
            session.stop()


def get_spark_session() -> SparkSession:
    """Get the current Spark session with proper error handling"""
    try:
        return SparkSessionManager.get_session()
    except Exception as e:
        if "JAVA_GATEWAY_EXITED" in str(e) or "Java Runtime" in str(e):
            raise RuntimeError(
                "Spark requires Java to be installed. Please install Java 8 or 11 and set JAVA_HOME environment variable.\n"
                "For macOS: brew install openjdk@11\n"
                "For Ubuntu: sudo apt-get install openjdk-11-jdk\n"
                "Then set: export JAVA_HOME=/path/to/java"
            ) from e
        else:
            raise


def get_spark_config_for_environment() -> Dict[str, str]:
    """Get Spark configuration based on current environment"""
    return SparkSessionManager._get_environment_config()


def is_development() -> bool:
    """Check if running in development environment"""
    return os.getenv("SPARK_ENV", "development") == "development"


def is_production() -> bool:
    """Check if running in production environment"""
    return os.getenv("SPARK_ENV", "development") == "production"


def is_testing() -> bool:
    """Check if running in testing environment"""
    return os.getenv("SPARK_ENV", "development") == "testing"


# Test-friendly session management
class TestableSparkSessionManager(SparkSessionManager):
    """Spark session manager that's easy to test with"""
    
    @classmethod
    def get_test_session(cls, app_name: str = "TestApp") -> SparkSession:
        """Get a session specifically configured for testing"""
        test_config = {
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "false"
        }
        
        return cls.get_session(app_name, "local[1]", test_config)
    
    @classmethod
    def reset_for_testing(cls):
        """Reset session for testing (call in setUp)"""
        cls.reset_session()
