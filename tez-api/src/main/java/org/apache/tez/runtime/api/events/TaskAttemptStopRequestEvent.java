package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;

public class TaskAttemptStopRequestEvent extends Event
  implements com.datamonad.mr3.api.EventToProcessor {

  public static final TaskAttemptStopRequestEvent INSTANCE = new TaskAttemptStopRequestEvent();

  public TaskAttemptStopRequestEvent() {
  }
}
