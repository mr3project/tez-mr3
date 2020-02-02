package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;

public class TaskAttemptDAGLeavingEvent extends Event
  implements com.datamonad.mr3.api.EventToProcessor {

  public final int dagIdId;

  public TaskAttemptDAGLeavingEvent(int dagIdId) {
    this.dagIdId = dagIdId;
  }
}
