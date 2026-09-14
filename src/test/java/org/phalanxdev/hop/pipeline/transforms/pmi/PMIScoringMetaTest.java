package org.phalanxdev.hop.pipeline.transforms.pmi;

import org.apache.hop.core.annotations.Transform;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PMIScoringMetaTest {

  @Test
  void testDefaults() {
    PMIScoringMeta meta = new PMIScoringMeta();
    meta.setDefault();

    assertFalse( meta.getFileNameFromField() );
    assertFalse( meta.getCacheLoadedModels() );
    assertFalse( meta.getOutputProbabilities() );
    assertFalse( meta.getUpdateIncrementalModel() );
  }

  @Test
  void testClone() {
    PMIScoringMeta meta = new PMIScoringMeta();
    meta.setDefault();
    meta.setSerializedModelFileName( "hdfs://path/to/model.model" );
    meta.setOutputProbabilities( true );

    PMIScoringMeta clone = (PMIScoringMeta) meta.clone();
    assertNotNull( clone );
    assertEquals( "hdfs://path/to/model.model", clone.getSerializedModelFileName() );
    assertTrue( clone.getOutputProbabilities() );
  }

  @Test
  void testDialogAndTransformAnnotation() {
    PMIScoringMeta meta = new PMIScoringMeta();
    assertEquals( "org.phalanxdev.hop.ui.pipeline.pmi.PMIScoringDialog", meta.getDialogClassName() );

    Transform transform = PMIScoringMeta.class.getAnnotation( Transform.class );
    assertNotNull( transform );
    assertEquals( "PMIScoring", transform.id() );
    assertEquals( "PMI", transform.categoryDescription() );
  }
}
