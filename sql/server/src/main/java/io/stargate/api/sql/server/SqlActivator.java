/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.api.sql.server;

import io.stargate.db.Persistence;
import java.util.Collection;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlActivator implements BundleActivator, ServiceListener {

  private static final Logger log = LoggerFactory.getLogger(SqlActivator.class);

  private static final String PERSISTENCE_IDENTIFIER =
      System.getProperty(
          "stargate.sql.persistence_id",
          System.getProperty("stargate.persistence_id", "CassandraPersistence"));
  private static final String PERSISTENCE_FILTER =
      String.format("(Identifier=%s)", PERSISTENCE_IDENTIFIER);

  private BundleContext context;
  private ServiceReference<Persistence> backendRef;
  private AvaticaServer server;

  @Override
  public void start(BundleContext context) {
    this.context = context;

    try {
      context.addServiceListener(this, PERSISTENCE_FILTER);
    } catch (InvalidSyntaxException ise) {
      throw new RuntimeException(ise);
    }

    maybeStart();
  }

  @Override
  public void stop(BundleContext context) {
    stopServer();
  }

  @Override
  public void serviceChanged(ServiceEvent event) {
    switch (event.getType()) {
      case (ServiceEvent.REGISTERED):
        maybeStart();
        break;

      case (ServiceEvent.UNREGISTERING):
        maybeStop(event.getServiceReference());
        break;
    }
  }

  private synchronized void maybeStop(ServiceReference<?> ref) {
    if (ref.equals(backendRef)) {
      log.info("Backend persistence became unavailable: {}", ref.getBundle());
      stopServer();
    }
  }

  private synchronized void stopServer() {
    AvaticaServer s = this.server;
    if (s != null) {
      log.info("Stopping Avatica Server");
      s.stop();
    }

    server = null;
    backendRef = null;
  }

  private synchronized void maybeStart() {
    if (server != null) {
      return;
    }

    Collection<ServiceReference<Persistence>> refs;
    try {
      refs = context.getServiceReferences(Persistence.class, PERSISTENCE_FILTER);
    } catch (InvalidSyntaxException e) {
      throw new IllegalStateException(e);
    }

    for (ServiceReference<Persistence> ref : refs) {
      Persistence backend = context.getService(ref);
      if (backend != null) {
        backendRef = ref;
        log.info("Using backend persistence: {}", ref.getBundle());

        server = new AvaticaServer(backend);
        log.info("Starting Avatica Server");
        server.start();
        break;
      }
    }
  }
}
