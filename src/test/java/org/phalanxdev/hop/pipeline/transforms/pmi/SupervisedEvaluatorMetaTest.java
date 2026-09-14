package org.phalanxdev.hop.pipeline.transforms.pmi;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupervisedEvaluatorMetaTest {

  @Test
  void testDefaultsAndSetters() {
    SupervisedEvaluatorMeta meta = new SupervisedEvaluatorMeta();
    meta.setDefault();

    assertEquals( "", meta.getClassName() );
    assertFalse( meta.isOutputIRStats() );
    assertFalse( meta.getOutputIRStats() );
    assertFalse( meta.isOutputAUC() );
    assertFalse( meta.getOutputAUC() );

    meta.setClassName( "targetClass" );
    meta.setOutputIRStats( true );
    meta.setOutputAUC( true );

    assertEquals( "targetClass", meta.getClassName() );
    assertTrue( meta.isOutputIRStats() );
    assertTrue( meta.getOutputIRStats() );
    assertTrue( meta.isOutputAUC() );
    assertTrue( meta.getOutputAUC() );
  }

  @Test
  void testAnnotationsAndDialog() {
    SupervisedEvaluatorMeta meta = new SupervisedEvaluatorMeta();
    assertNotNull( meta.getDialogClassName() );
    assertEquals( "org.phalanxdev.hop.ui.pipeline.pmi.SupervisedEvaluatorDialog", meta.getDialogClassName() );

    Transform transform = SupervisedEvaluatorMeta.class.getAnnotation( Transform.class );
    assertNotNull( transform );
    assertEquals( "SupervisedEvaluator", transform.id() );
    assertEquals( "PMI", transform.categoryDescription() );

    GuiPlugin guiPlugin = SupervisedEvaluatorMeta.class.getAnnotation( GuiPlugin.class );
    assertNotNull( guiPlugin );
  }
}
