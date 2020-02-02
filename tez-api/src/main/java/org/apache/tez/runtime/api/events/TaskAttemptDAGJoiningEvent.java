package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;

public final class TaskAttemptDAGJoiningEvent extends Event
  implements com.datamonad.mr3.api.EventToProcessor {

  public final int dagIdId;

  public TaskAttemptDAGJoiningEvent(int dagIdId) {
    this.dagIdId = dagIdId;
  }
}
