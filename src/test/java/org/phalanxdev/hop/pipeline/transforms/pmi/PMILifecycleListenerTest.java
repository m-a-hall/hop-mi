package org.phalanxdev.hop.pipeline.transforms.pmi;

import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class PMILifecycleListenerTest {

  @BeforeAll
  static void setUp() {
    HopLogStore.init();
  }

  @Test
  void testCallExtensionPointDoesNotThrowWhenAutoInstallDisabled() {
    PMILifecycleListener listener = new PMILifecycleListener();
    ILogChannel log = new LogChannel("PMILifecycleListenerTest");
    IVariables variables = new Variables();
    variables.setVariable(PMILifecycleListener.HOP_MI_AUTO_INSTALL_PACKAGES, "N");

    assertDoesNotThrow(() -> listener.callExtensionPoint(log, variables, null));
  }
}
