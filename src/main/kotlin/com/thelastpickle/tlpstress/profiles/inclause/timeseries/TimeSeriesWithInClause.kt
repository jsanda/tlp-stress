package com.thelastpickle.tlpstress.profiles.inclause.timeseries

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.generators.Field
import com.thelastpickle.tlpstress.generators.FieldGenerator
import com.thelastpickle.tlpstress.generators.functions.Random
import com.thelastpickle.tlpstress.profiles.IStressProfile
import com.thelastpickle.tlpstress.profiles.IStressRunner
import com.thelastpickle.tlpstress.profiles.Operation

class TimeSeriesWithInClause : IStressProfile {

    override fun schema(): List<String> {
        val query = """CREATE TABLE IF NOT EXISTS sensor_data (
                            sensor_id text,
                            timestamp timeuuid,
                            data text,
                            primary key(sensor_id, timestamp))
                            WITH CLUSTERING ORDER BY (timestamp DESC)
                           """.trimIndent()

        return listOf(query)
    }

    lateinit var prepared: PreparedStatement
    lateinit var getRow: PreparedStatement
    lateinit var getPartitionHead: PreparedStatement

    override fun prepare(session: Session) {
        prepared = session.prepare("INSERT INTO sensor_data (sensor_id, timestamp, data) VALUES (?, ?, ?)")
        getRow = session.prepare("SELECT * FROM sensor_data WHERE sensor_id IN ? AND timestamp < ?")
        getPartitionHead = session.prepare("SELECT * from sensor_data WHERE sensor_id = ? LIMIT ?")
    }

    override fun getRunner(context: StressContext): IStressRunner {

        val dataField = context.registry.getGenerator("sensor_data", "data")

        class TimeSeriesRunner(val insert: PreparedStatement, val select: PreparedStatement, val limit: Int) : IStressRunner {

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val ids: MutableList<String> = mutableListOf()
                val startId = partitionKey.id - 25
                for (id in startId..partitionKey.id) {
                    ids.add(partitionKey.prefix + id)
                }
                val bound = select.bind(ids, UUIDs.endOf(System.currentTimeMillis()))
                return Operation.SelectStatement(bound)
            }

            override fun getNextMutation(partitionKey: PartitionKey) : Operation {
                val data = dataField.getText()
                val timestamp = UUIDs.timeBased()
                val bound = insert.bind(partitionKey.getText(),timestamp, data)
                return Operation.Mutation(bound)
            }

        }

        return TimeSeriesRunner(prepared, getRow, 500)
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        return mapOf(Field("sensor_data", "data") to Random().apply { min=100; max=200 })
    }

}