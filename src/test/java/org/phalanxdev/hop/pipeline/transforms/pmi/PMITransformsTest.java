package org.phalanxdev.hop.pipeline.transforms.pmi;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.phalanxdev.hop.pipeline.transforms.pmi.weka.PMIFlowExecutorMeta;
import org.phalanxdev.hop.pipeline.transforms.pmi.weka.PMIForecastingMeta;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PMITransformsTest {

  private static final List<Class<? extends ITransformMeta>> ALL_TRANSFORM_METAS = List.of(
      PMILinearRegression.class,
      PMIMultiLayerPerceptronRegressor.class,
      PMIScoringMeta.class,
      PMILogisticRegression.class,
      PMIDecisionTreeClassifier.class,
      PMIDeepLearningNetwork.class,
      PMIRandomForestClassifier.class,
      PMISVMRegressor.class,
      PMIXGBClassifier.class,
      PMIGradientBoostedTrees.class,
      SupervisedEvaluatorMeta.class,
      PMIRandomForestRegressor.class,
      PMINaiveBayes.class,
      PMISVMClassifier.class,
      PMIMultiLayerPerceptronClassifier.class,
      PMIDecisionTreeRegressor.class,
      PMINaiveBayesIncremental.class,
      PMIXGBRegressor.class,
      PMINaiveBayesMultinomial.class,
      PMIFlowExecutorMeta.class,
      PMIForecastingMeta.class
  );

  @Test
  void testAllTransformsHaveValidAnnotationsAndDialogs() throws Exception {
    for ( Class<? extends ITransformMeta> clazz : ALL_TRANSFORM_METAS ) {
      Transform transform = clazz.getAnnotation( Transform.class );
      assertNotNull( transform, "Class " + clazz.getSimpleName() + " should be annotated with @Transform" );
      assertFalse( transform.id().isBlank(), "Transform id should not be blank for " + clazz.getSimpleName() );
      assertFalse( transform.name().isBlank(), "Transform name should not be blank for " + clazz.getSimpleName() );
      assertEquals( "PMI", transform.categoryDescription(), "Category should be PMI for " + clazz.getSimpleName() );

      ITransformMeta instance = clazz.getDeclaredConstructor().newInstance();
      String dialogClassName = instance.getDialogClassName();
      assertNotNull( dialogClassName, "Dialog class name must not be null for " + clazz.getSimpleName() );
      assertFalse( dialogClassName.isBlank(), "Dialog class name must not be blank for " + clazz.getSimpleName() );

      // Verify the dialog class can be found on classpath
      Class<?> dialogClass = Class.forName( dialogClassName );
      assertNotNull( dialogClass, "Dialog class " + dialogClassName + " must exist on classpath" );
    }
  }

  @Test
  void testSchemeTransformsInstantiateWithScheme() {
    List<BaseSupervisedPMIMeta> schemeTransforms = List.of(
        new PMILinearRegression(),
        new PMIMultiLayerPerceptronRegressor(),
        new PMILogisticRegression(),
        new PMIDecisionTreeClassifier(),
        new PMIDeepLearningNetwork(),
        new PMIRandomForestClassifier(),
        new PMISVMRegressor(),
        new PMIXGBClassifier(),
        new PMIGradientBoostedTrees(),
        new PMIRandomForestRegressor(),
        new PMINaiveBayes(),
        new PMISVMClassifier(),
        new PMIMultiLayerPerceptronClassifier(),
        new PMIDecisionTreeRegressor(),
        new PMINaiveBayesIncremental(),
        new PMIXGBRegressor(),
        new PMINaiveBayesMultinomial()
    );

    for ( BaseSupervisedPMIMeta meta : schemeTransforms ) {
      assertNotNull( meta.getSchemeName(), "Scheme name should not be null for " + meta.getClass().getSimpleName() );
      assertFalse( meta.getSchemeName().isBlank(), "Scheme name should not be blank for " + meta.getClass().getSimpleName() );
    }
  }
}
