package com.bazaaring.snowflake

import java.time.Clock
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * 64-bit ID Layout:
 *
 *  1  bit  = sign
 *  31 bits = seconds
 *  5  bits = keyspace
 *  5  bits = datacenter
 *  8  bits = instance number
 *  14 bits = sequence number
 */
class Snowflake(
    private val clock: Clock = Clock.systemUTC(),
    private val datacenter: Int,
    private val instanceNumber: (Int) -> Int,
) {

    enum class Layout(val bits: Int) {

        SIGN(bits = 1),
        SECONDS(bits = 31),
        KEYSPACE(bits = 5),
        DATACENTER(bits = 5),
        INSTANCE_NUMBER(bits = 8),
        SEQUENCE_NUMBER(bits = 14);

        val mask = (1L.shl(bits) - 1).toInt()

        val shiftCount by lazy {
            (ordinal + 1 until values().size).sumOf { values()[it].bits }
        }

    }

    init {
        require(datacenter in 0..Layout.DATACENTER.mask) {
            "datacenter must be in range [0, ${Layout.DATACENTER.mask}]"
        }
    }

    private val second = AtomicLong(0L)
    private val sequenceRef = AtomicReference<Pair<Long, AtomicInteger>>()

    @Throws(SnowflakeLimitReachedException::class)
    fun nextId(keyspace: Int, epochSecond: Long): Long {

        require(keyspace in 0..Layout.KEYSPACE.mask) {
            "keyspace must be in range [0, ${Layout.KEYSPACE.mask}]"
        }

        val instance = instanceNumber.invoke(Layout.INSTANCE_NUMBER.mask + 1)
        check(instance in 0..Layout.INSTANCE_NUMBER.mask) {
            "instance must be in range [0, ${Layout.INSTANCE_NUMBER.mask}]"
        }

        val prevSec = second.get()
        val currSec = clock.instant().epochSecond

        require(epochSecond in 0..currSec) {
            "epoch second must be in range [0, ${currSec}]"
        }

        val sec = currSec - epochSecond
        if (sec > Layout.SECONDS.mask)
            throw SnowflakeLimitReachedException("second > ${Layout.SECONDS.mask}")

        if (!second.compareAndSet(prevSec, currSec))
            return nextId(keyspace, epochSecond)

        val sequence = getSequence(currSec)
            ?: return nextId(keyspace, epochSecond)

        val seqNum = sequence.getAndUpdate { num ->
            if (num + 1 > Layout.SEQUENCE_NUMBER.mask)
                throw SnowflakeLimitReachedException("sequence number > ${Layout.SEQUENCE_NUMBER.mask}")
            num + 1
        }

        return makeId(0, sec.toInt(), keyspace, datacenter, instance, seqNum)
    }

    private fun getSequence(version: Long): AtomicInteger? =
        try {
            sequenceRef.updateAndGet { pair ->
                val sequence = when {
                    pair == null -> AtomicInteger()
                    pair.first < version -> AtomicInteger()
                    pair.first == version -> pair.second
                    else -> error("")
                }
                Pair(version, sequence)
            }.second
        } catch (ex: Exception) {
            null
        }

    private fun makeId(vararg values: Int): Long =
        Layout.values().fold(0L) { id, item ->
            id or values[item.ordinal].toLong().shl(item.shiftCount)
        }

    companion object {
        fun parseId(value: Long): Map<Layout, Int> =
            Layout.values().associateWith { item ->
                value.shr(item.shiftCount).toInt() and item.mask
            }
    }

}
