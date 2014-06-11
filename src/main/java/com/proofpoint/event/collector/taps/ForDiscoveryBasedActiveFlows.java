package com.proofpoint.event.collector.taps;

import javax.inject.Qualifier;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(PARAMETER)
@Qualifier
@interface ForDiscoveryBasedActiveFlows
{
}
