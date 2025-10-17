"""
Spark Configuration for Different Environments
Centralized configuration management for Spark sessions
"""

import os
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class SparkConfig:
    """Spark configuration for a specific environment"""
    master: str
    app_name: str
    config: Dict[str, str]
    memory_settings: Optional[Dict[str, str]] = None


class SparkConfigManager:
    """Manages Spark configurations for different environments"""
    
    # Development configuration
    DEVELOPMENT = SparkConfig(
        master="local[*]",
        app_name="MarketDataETL-Dev",
        config={
            "spark.sql.adaptive.enabled": "false",  # Easier debugging
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            "spark.sql.adaptive.localShuffleReader.enabled": "false"
        },
        memory_settings={
            "spark.driver.memory": "2g",
            "spark.driver.maxResultSize": "1g",
            "spark.executor.memory": "2g"
        }
    )
    
    # Testing configuration
    TESTING = SparkConfig(
        master="local[1]",  # Single core for testing
        app_name="MarketDataETL-Test",
        config={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.adaptive.skewJoin.enabled": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "1000",
            "spark.sql.adaptive.localShuffleReader.enabled": "false"
        },
        memory_settings={
            "spark.driver.memory": "1g",
            "spark.driver.maxResultSize": "512m",
            "spark.executor.memory": "1g"
        }
    )
    
    # Production configuration
    PRODUCTION = SparkConfig(
        master="yarn",
        app_name="MarketDataETL-Prod",
        config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "2",
            "spark.dynamicAllocation.maxExecutors": "20",
            "spark.dynamicAllocation.initialExecutors": "5",
            "spark.dynamicAllocation.executorIdleTimeout": "60s",
            "spark.dynamicAllocation.cachedExecutorIdleTimeout": "300s"
        },
        memory_settings={
            "spark.driver.memory": "4g",
            "spark.driver.maxResultSize": "2g",
            "spark.executor.memory": "4g",
            "spark.executor.memoryFraction": "0.8"
        }
    )
    
    # Cluster configuration
    CLUSTER = SparkConfig(
        master="spark://master:7077",
        app_name="MarketDataETL-Cluster",
        config={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
            "spark.sql.adaptive.localShuffleReader.enabled": "true"
        },
        memory_settings={
            "spark.driver.memory": "2g",
            "spark.driver.maxResultSize": "1g",
            "spark.executor.memory": "2g"
        }
    )
    
    @classmethod
    def get_config(cls, environment: Optional[str] = None) -> SparkConfig:
        """
        Get Spark configuration for the specified environment
        
        Args:
            environment: Environment name (development, testing, production, cluster)
                        If None, will auto-detect from environment variables
            
        Returns:
            SparkConfig object for the environment
        """
        if environment is None:
            environment = os.getenv("SPARK_ENV", "development")
        
        configs = {
            "development": cls.DEVELOPMENT,
            "testing": cls.TESTING,
            "production": cls.PRODUCTION,
            "cluster": cls.CLUSTER
        }
        
        if environment not in configs:
            raise ValueError(f"Unknown environment: {environment}. "
                           f"Available: {list(configs.keys())}")
        
        return configs[environment]
    
    @classmethod
    def get_custom_config(cls, 
                         master: str,
                         app_name: str,
                         config: Dict[str, str],
                         memory_settings: Optional[Dict[str, str]] = None) -> SparkConfig:
        """
        Create a custom Spark configuration
        
        Args:
            master: Spark master URL
            app_name: Application name
            config: Spark configuration dictionary
            memory_settings: Memory-related settings
            
        Returns:
            Custom SparkConfig object
        """
        return SparkConfig(
            master=master,
            app_name=app_name,
            config=config,
            memory_settings=memory_settings
        )


def get_spark_config() -> SparkConfig:
    """Get the current Spark configuration based on environment"""
    return SparkConfigManager.get_config()


def get_spark_config_for_environment(env: str) -> SparkConfig:
    """Get Spark configuration for a specific environment"""
    return SparkConfigManager.get_config(env)


# Environment-specific helper functions
def is_development() -> bool:
    """Check if running in development environment"""
    return os.getenv("SPARK_ENV", "development") == "development"


def is_production() -> bool:
    """Check if running in production environment"""
    return os.getenv("SPARK_ENV", "development") == "production"


def is_testing() -> bool:
    """Check if running in testing environment"""
    return os.getenv("SPARK_ENV", "development") == "testing"


# Example usage
if __name__ == "__main__":
    print("Spark Configuration Examples")
    print("=" * 40)
    
    # Get current environment config
    config = get_spark_config()
    print(f"Current environment: {os.getenv('SPARK_ENV', 'development')}")
    print(f"Master: {config.master}")
    print(f"App Name: {config.app_name}")
    print(f"Config keys: {list(config.config.keys())}")
    
    # Show different environment configs
    print("\nAvailable configurations:")
    for env in ["development", "testing", "production", "cluster"]:
        try:
            env_config = get_spark_config_for_environment(env)
            print(f"  {env}: {env_config.master} - {env_config.app_name}")
        except ValueError as e:
            print(f"  {env}: {e}")
