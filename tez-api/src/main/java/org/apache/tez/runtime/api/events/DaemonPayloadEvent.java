package org.apache.tez.runtime.api.events;

import com.google.protobuf.ByteString;
import org.apache.tez.runtime.api.Event;

public final class DaemonPayloadEvent extends Event
  implements com.datamonad.mr3.api.EventToProcessor {

  public final ByteString payload;

  public DaemonPayloadEvent (ByteString payload) {
    this.payload = payload;
  }
}
