package com.bazaaring.snowflake

import com.bazaaring.k8s.K8sPodNumberAssigner
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@AutoConfiguration
@EnableConfigurationProperties(SnowflakeProperties::class)
class SnowflakeAutoConfiguration {

    @Configuration
    @ConditionalOnClass(K8sPodNumberAssigner::class)
    class K8sConfiguration(private val props: SnowflakeProperties) {

        @Bean
        @ConditionalOnBean(K8sPodNumberAssigner::class)
        fun k8sSnowflake(podNumberAssigner: K8sPodNumberAssigner): Snowflake {
            return Snowflake(
                datacenter = props.datacenter,
                instanceNumber = { numberOfInstances ->
                    podNumberAssigner.getPodNumber(props.k8s.deployment, length = numberOfInstances)
                },
            )
        }

    }

}
