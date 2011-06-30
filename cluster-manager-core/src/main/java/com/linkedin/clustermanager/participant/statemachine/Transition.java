package com.linkedin.clustermanager.participant.statemachine;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface Transition
{
  String from();

  String to();

}
