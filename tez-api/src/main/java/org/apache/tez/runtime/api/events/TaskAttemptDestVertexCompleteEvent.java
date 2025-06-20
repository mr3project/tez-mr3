package org.apache.tez.runtime.api.events;

import org.apache.tez.runtime.api.Event;

public class TaskAttemptDestVertexCompleteEvent extends Event
  implements com.datamonad.mr3.api.EventToProcessor {

  public final int dagIdId, vertexIdId;

  public TaskAttemptDestVertexCompleteEvent(int dagIdId, int vertexIdId) {
    this.dagIdId = dagIdId;
    this.vertexIdId = vertexIdId;
  }
}
