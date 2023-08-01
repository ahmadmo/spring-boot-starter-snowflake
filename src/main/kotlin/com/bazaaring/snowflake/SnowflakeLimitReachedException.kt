package com.bazaaring.snowflake

class SnowflakeLimitReachedException(override val message: String) : Exception(message)
