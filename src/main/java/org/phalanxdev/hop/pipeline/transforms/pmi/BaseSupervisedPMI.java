/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.phalanxdev.hop.pipeline.transforms.pmi;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.phalanxdev.hop.utils.ArffMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.phalanxdev.hop.pipeline.transforms.reservoirsampling.ReservoirSamplingData;
import org.phalanxdev.mi.Evaluator;
import org.phalanxdev.mi.PMIEngine;
import weka.core.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for PMI supervised classification/regression-based steps. Provides all the step logic for establishing the
 * engine to use, scheme implementation from the engine, and row-handling logic.
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 */
public class BaseSupervisedPMI extends BaseTransform<BaseSupervisedPMIMeta, BaseSupervisedPMIData>
    implements ITransform<BaseSupervisedPMIMeta, BaseSupervisedPMIData> {

  private static Class<?> PKG = BaseSupervisedPMIMeta.class;

  protected BaseSupervisedPMIMeta m_meta;
  protected BaseSupervisedPMIData m_data;

  protected boolean m_trainingDone;
  protected boolean m_testingDone;

  public BaseSupervisedPMI( TransformMeta transformMeta, BaseSupervisedPMIMeta meta, BaseSupervisedPMIData data,
      int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    m_meta = meta;
    m_data = data;
  }

  /**
   * Initializes the step and performs configuration checks
   *
   * @return true if all is good.
   */
  @Override public boolean init() {
    if ( super.init() ) {

      m_trainingDone = false;
      m_testingDone = true;

      try {
        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getEngineName() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoEngineSpecified" ) );
        }

        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getSchemeName() ) ) {
          throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoSchemeSpecified" ) );
        }

        // check engine availability
        String engineName = environmentSubstitute( m_meta.getEngineName() );
        PMIEngine.init();
        m_data.m_engine = PMIEngine.getEngine( engineName );

        List<String> engineMessages = new ArrayList<String>();
        if ( !m_data.m_engine.engineAvailable( engineMessages ) ) {
          StringBuilder b = new StringBuilder();
          b.append(
              BaseMessages.getString( PKG, "BasePMIStep.Error.EngineNotAvailable", m_data.m_engine.engineName() ) )
              .append( "\n" );
          for ( String message : engineMessages ) {
            b.append( message ).append( "\n" );
          }
          throw new HopException( b.toString() );
        }

        m_data.m_scheme = m_data.m_engine.getScheme( environmentSubstitute( m_meta.getSchemeName() ) );
        if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getSchemeCommandLineOptions() ) ) {
          m_data.m_scheme.setSchemeOptions( Utils.splitOptions( m_meta.getSchemeCommandLineOptions() ) );
        }

        m_data.m_scheme.setSamplingConfigs( m_meta.getSamplingConfigs() );
        m_data.m_scheme.setPreprocessingConfigs( m_meta.getPreprocessingConfigs() );

        String rowHandlingMode = environmentSubstitute( m_meta.getRowsToProcess() );
        for ( BaseSupervisedPMIData.RowHandlingMode m : BaseSupervisedPMIData.RowHandlingMode.values() ) {
          if ( m.toString().equalsIgnoreCase( rowHandlingMode ) ) {
            m_data.m_rowHandlingMode = m;
            break;
          }
        }

        if ( m_data.m_rowHandlingMode == null ) {
          throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoRowHandlingSpecified" ) );
        }

        if ( m_meta.getFieldMetadata() == null || m_meta.getFieldMetadata().size() == 0 ) {
          throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoModellingFieldsSpecified" ) );
        }

        // Do we have input?
        List<IStream> infoStreams = m_meta.getStepIOMeta().getInfoStreams();
        if ( infoStreams.size() == 0 ) {
          throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoIncomingData" ) );
        }
        String trainingInputStepName = environmentSubstitute( m_meta.getTrainingStepInputName() );
        if ( infoStreams.size() > 1 ) {
          // check that there is a training stream named
          if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getTrainingStepInputName() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoTrainingInputStep" ) );
          }

          boolean trainingMatch = false;
          for ( IStream input : infoStreams ) {
            if ( environmentSubstitute( input.getSubject().toString() ).equals( trainingInputStepName ) ) {
              trainingMatch = true;
              m_data.m_trainingStream = input;
              break;
            }
          }
          if ( !trainingMatch ) {
            throw new HopException( BaseMessages
                .getString( PKG, "BasePMIStep.Error.UnableToFindNamedTrainingSource", trainingInputStepName ) );
          }

          if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getTestingStepInputName() )
              && m_meta.getEvalMode() != Evaluator.EvalMode.NONE ) {
            // check for the test source
            String testInputStepName = environmentSubstitute( m_meta.getTestingStepInputName() );
            boolean testMatch = false;
            for ( IStream input : infoStreams ) {
              if ( environmentSubstitute( input.getSubject().toString() ).equals( testInputStepName ) ) {
                testMatch = true;
                m_data.m_testStream = input;
                break;
              }
            }
            if ( !testMatch ) {
              throw new HopException(
                  BaseMessages.getString( PKG, "BasePMIStep.Error.UnableToFindNamedTestSource", testInputStepName ) );
            }
          }
        } else {
          m_data.m_trainingStream = infoStreams.get( 0 );
          if ( m_data.m_trainingStream == null ) {
            throw new HopException( BaseMessages
                .getString( PKG, "BasePMIStep.Error.UnableToFindNamedTrainingSource", trainingInputStepName ) );
          }
        }

        m_data.m_trainingRowMeta =
            getPipelineMeta().getTransformFields( (String) m_data.m_trainingStream.getSubject() );
        List<String> trainingFieldNames = Arrays.asList( m_data.m_trainingRowMeta.getFieldNames() );
        m_data.m_testingRowMeta = null;
        List<String> testingFieldNames = null;
        if ( m_data.m_testStream != null ) {
          m_data.m_testingRowMeta = getPipelineMeta().getTransformFields( (String) m_data.m_testStream.getSubject() );
          testingFieldNames = Arrays.asList( m_data.m_testingRowMeta.getFieldNames() );
        }

        // class validation
        if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getClassField() ) ) {
          String classFieldName = environmentSubstitute( m_meta.getClassField() );
          if ( !trainingFieldNames.contains( classFieldName ) ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.TrainingDataDoesNotContainClass", classFieldName ) );
          }
          m_data.m_classIndex = trainingFieldNames.indexOf( classFieldName );
          for ( ArffMeta am : m_meta.getFieldMetadata() ) {
            if ( am.getFieldName().equals( classFieldName ) ) {
              m_data.m_classArffMeta = am;
              break;
            }
          }
          if ( m_data.m_classArffMeta == null ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.UnableToFindClassNameInArffMetas", classFieldName ) );
          }
        } else {
          m_data.m_classArffMeta = m_meta.getFieldMetadata().get( m_meta.getFieldMetadata().size() - 1 );
          m_data.m_classIndex =
              trainingFieldNames
                  .indexOf( m_data.m_classArffMeta.getFieldName() ); // assume class is last in list of *ArffMetas*
        }

        if ( m_meta.getEvalMode() == Evaluator.EvalMode.SEPARATE_TEST_SET && m_data.m_testStream != null ) {
          // does the class field from the training data exist in the test data?
          if ( testingFieldNames.indexOf( m_data.m_classArffMeta.getFieldName() ) < 0 ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.TestingDataDoesNotContainClass",
                m_data.m_classArffMeta.getFieldName() ) );
          }
          m_data.m_separateTestClassIndex = testingFieldNames.indexOf( m_data.m_classArffMeta.getFieldName() );

          // check that types match
          IValueMeta trainingClassVM = m_data.m_trainingRowMeta.getValueMeta( m_data.m_classIndex );
          IValueMeta testClassVM = m_data.m_testingRowMeta.getValueMeta( m_data.m_separateTestClassIndex );

          if ( trainingClassVM.getType() != testClassVM.getType() ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.ClassTypeTrainingDoesNotMatchClassTypeSeparateTest" ) );
          }
        }

        // input field --> model field validation (i.e. are the fields defined in the ARFF metas actually in the incoming data?)
        List<ArffMeta> modelFields = m_meta.getFieldMetadata();
        m_data.m_trainingFieldIndexes.clear();
        StringBuilder missingFieldWarnings = new StringBuilder();
        for ( int i = 0; i < modelFields.size(); i++ ) {
          String nameToFind = modelFields.get( i ).getFieldName();
          if ( trainingFieldNames.indexOf( nameToFind ) < 0 ) {
            missingFieldWarnings.append( nameToFind ).append( " " );
          } else {
            m_data.m_trainingFieldIndexes.put( nameToFind, trainingFieldNames.indexOf( nameToFind ) );
          }
        }

        if ( m_data.m_trainingFieldIndexes.size() < m_meta.getFieldMetadata().size() / 2 ) {
          throw new HopException(
              BaseMessages.getString( PKG, "BasePMIStep.Error.NumInputFieldsPresentInTrainingIsLessThanHalf" ) );
        }

        if ( missingFieldWarnings.length() > 0 ) {
          logBasic( BaseMessages.getString( PKG, "BasePMIStep.Warning.MissingInputFieldsTraining",
              missingFieldWarnings.toString().trim() ) );
        }

        if ( m_meta.getEvalMode() == Evaluator.EvalMode.SEPARATE_TEST_SET && m_data.m_testStream != null ) {
          m_data.m_testingFieldIndexes.clear();
          missingFieldWarnings = new StringBuilder();

          for ( int i = 0; i < modelFields.size(); i++ ) {
            String nameToFind = modelFields.get( i ).getFieldName();
            if ( testingFieldNames.indexOf( nameToFind ) < 0 ) {
              missingFieldWarnings.append( nameToFind ).append( " " );
            } else {
              m_data.m_testingFieldIndexes.put( nameToFind, testingFieldNames.indexOf( nameToFind ) );
            }
          }

          if ( m_data.m_testingFieldIndexes.size() < m_meta.getFieldMetadata().size() / 2 ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.NumInputFieldsPresentInSeparateTestIsLessThanHalf" ) );
          }

          if ( missingFieldWarnings.length() > 0 ) {
            logBasic( BaseMessages.getString( PKG, "BasePMIStep.Warning.MissingInputFieldsTesting" ),
                missingFieldWarnings.toString().trim() );
          }

          // now do a type match between test fields and training fields
          StringBuilder typeMismatch = new StringBuilder();
          for ( int i = 0; i < modelFields.size(); i++ ) {
            String nameToFind = modelFields.get( i ).getFieldName();
            if ( testingFieldNames.contains( nameToFind ) && trainingFieldNames.contains( nameToFind ) ) {
              IValueMeta trainingVM = m_data.m_trainingRowMeta.getValueMeta( trainingFieldNames.indexOf( nameToFind ) );
              IValueMeta testingVM = m_data.m_testingRowMeta.getValueMeta( testingFieldNames.indexOf( nameToFind ) );
              if ( trainingVM.getType() != testingVM.getType() ) {
                typeMismatch.append( nameToFind ).append( " " );
              }
            }
          }
          if ( typeMismatch.length() > 0 ) {
            throw new HopException( BaseMessages
                .getString( PKG, "BasePMIStep.Error.TypeMismatchBetweenTrainingFieldsAndSeparateTestFields",
                    typeMismatch.toString().trim() ) );
          }
        }

        // validate some row handling stuff
        if ( m_data.m_rowHandlingMode == BaseSupervisedPMIData.RowHandlingMode.Batch ) {
          if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getBatchSize() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoBatchSizeSpecified" ) );
          }
          m_data.m_batchSize = Integer.parseInt( environmentSubstitute( m_meta.getBatchSize() ) );
          if ( m_data.m_batchSize <= 0 ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.BatchSizeMustBeGreaterThanZero" ) );
          }
          m_data.m_trainingRows = new ArrayList<>( m_data.m_batchSize );
        } else if ( m_data.m_rowHandlingMode == BaseSupervisedPMIData.RowHandlingMode.Stratified ) {
          if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getStratificationFieldName() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoStratificationFieldSpecified" ) );
          }
          // now check that this field is present in the input
          String stratificationField = environmentSubstitute( m_meta.getStratificationFieldName() );
          if ( !trainingFieldNames.contains( stratificationField ) ) {
            throw new HopException( BaseMessages
                .getString( PKG, "BasePMIStep.Error.TrainingDataDoesNotContainStratificationField",
                    stratificationField ) );
          }
          m_data.m_stratificationIndex = trainingFieldNames.indexOf( stratificationField );
          m_data.m_stratificationFieldName = stratificationField;
          if ( m_data.m_rowHandlingMode == BaseSupervisedPMIData.RowHandlingMode.Stratified
              && m_data.m_testStream != null ) {
            if ( !testingFieldNames.contains( stratificationField ) ) {
              throw new HopException( BaseMessages
                  .getString( PKG, "BasePMIStep.Error.TestDataDoesNotContainStratificationField",
                      stratificationField ) );
            }
            m_data.m_separateTestStratificationIndex = testingFieldNames.indexOf( stratificationField );

            // check type against training
            IValueMeta stratTrainVM = m_data.m_trainingRowMeta.getValueMeta( m_data.m_stratificationIndex );
            IValueMeta stratTestVM = m_data.m_testingRowMeta.getValueMeta( m_data.m_separateTestStratificationIndex );
            if ( stratTestVM.getType() != stratTestVM.getType() ) {
              throw new HopException( BaseMessages
                  .getString( PKG, "BasePMIStep.Error.TrainingStratTypeDoesNotMatchSeparateTestStratType" ) );
            }
          }
        }

        if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getRandomSeed() ) ) {
          m_data.m_randomSeed = Integer.parseInt( environmentSubstitute( m_meta.getRandomSeed() ) );
        }

        // validate reservoir sampling
        if ( m_meta.getUseReservoirSampling() ) {
          if ( m_data.m_rowHandlingMode == BaseSupervisedPMIData.RowHandlingMode.Batch ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.ReservoirSamplingOnlyUsedWithAllOrStratified" ) );
          }

          if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getReservoirSize() ) ) {
            throw new HopException( BaseMessages.getString( PKG, "BasePMIStep.Error.NoReservoirSizeSpecified" ) );
          }

          String reservoirSizeS = environmentSubstitute( m_meta.getReservoirSize() );
          m_data.m_reservoirSize = Integer.parseInt( reservoirSizeS );
          if ( m_data.m_reservoirSize <= 0 ) {
            throw new HopException(
                BaseMessages.getString( PKG, "BasePMIStep.Error.ReservoirSizeMustBeGreaterThanZero" ) );
          }

          if ( m_data.m_rowHandlingMode != BaseSupervisedPMIData.RowHandlingMode.Stratified ) {
            m_data.m_trainingSampler = new ReservoirSamplingData();
            m_data.m_trainingSampler.setProcessingMode( ReservoirSamplingData.PROC_MODE.SAMPLING );
            m_data.m_trainingSampler.initialize( m_data.m_reservoirSize, m_data.m_randomSeed );
          } else {
            // can't pre-initialize the stratified samplers as we won't know the stratification values until
            // actually processing rows at runtime
          }
        }

        // evaluation-related stuff
        if ( m_meta.getEvalMode() != Evaluator.EvalMode.NONE ) {
          if ( m_meta.getEvalMode() == Evaluator.EvalMode.SEPARATE_TEST_SET ) {
            m_testingDone = false;
            if ( m_data.m_testStream == null ) {
              throw new HopException(
                  BaseMessages.getString( PKG, "BasePMIStep.Error.SeparateTestEvalButNoTestDataset" ) );
            }

            if ( m_data.m_rowHandlingMode == BaseSupervisedPMIData.RowHandlingMode.Batch ) {
              // Can only evaluate with a separate test set via All/Stratified
              throw new HopException( BaseMessages
                  .getString( PKG, "BasePMIStep.Error.SeparateTestSetEvalCantBeUsedWithBatchRowHandling" ) );
            }
          } else if ( m_meta.getEvalMode() == Evaluator.EvalMode.CROSS_VALIDATION ) {
            if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getXValFolds() ) ) {
              m_data.m_xValFolds = Integer.parseInt( environmentSubstitute( m_meta.getXValFolds() ) );
            }
          } else if ( m_meta.getEvalMode() == Evaluator.EvalMode.PERCENTAGE_SPLIT ) {
            if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getPercentageSplit() ) ) {
              m_data.m_percentageSplit = Integer.parseInt( environmentSubstitute( m_meta.getPercentageSplit() ) );
            }
          }

          // check for AUC/IR and whether user has specified legal class labels
          if ( m_meta.getOutputAUCMetrics() || m_meta.getOutputIRMetrics() ) {
            if ( m_data.m_classArffMeta.getArffType() != ArffMeta.NOMINAL ) {
              throw new HopException( BaseMessages
                  .getString( PKG, "BasePMIStep.Error.AUCIRMetricsRequestedButClassNotNominal",
                      m_data.m_classArffMeta.getFieldName() ) );
            }

            // this is necessary so that the output format can be determined in advance of seeing input rows
            // TODO - check for indexed values at this point, as these can be used in determining the output format
            if ( org.apache.hop.core.util.Utils.isEmpty( m_data.m_classArffMeta.getNominalVals() ) ) {
              throw new HopException(
                  BaseMessages.getString( PKG, "BasePMIStep.Error.IRAUCOutputSelectedButNoLegalClassValues" ) );
            }
          }
        }

        // model output
        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getModelOutputPath() ) ) {
          logBasic( BaseMessages.getString( PKG, "BasePMIStep.Warning.NoModelPathSupplied" ) );
        } else {
          m_data.m_modelOutputPath = environmentSubstitute( m_meta.getModelOutputPath() );
        }
        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getModelFileName() ) ) {
          logBasic( BaseMessages.getString( PKG, "BasePMIStep.Warning.NoModelFileNameSupplied" ) );
        } else {
          m_data.m_modelFileName = environmentSubstitute( m_meta.getModelFileName() );
        }

        // incremental scheme?
        m_data.checkForIncrementalTraining( m_meta, getLogChannel() );

        // Load resumable?
        if ( m_data.m_scheme.supportsResumableTraining() && !org.apache.hop.core.util.Utils
            .isEmpty( m_meta.getResumableModelPath() ) && ( m_meta.getEvalMode() == Evaluator.EvalMode.NONE
            || m_meta.getEvalMode() == Evaluator.EvalMode.SEPARATE_TEST_SET ) ) {
          String modelPath = environmentSubstitute( m_meta.getResumableModelPath() );
          // TODO
        }
      } catch ( Exception ex ) {
        logError( ex.getMessage(), ex );
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Row processing logic
   *
   * @return false if there is no more processing to be done
   * @throws HopException if a problem occurs
   */
  @Override public boolean processRow() throws HopException {

    if ( first ) {
      first = false;
      m_data.m_outputRowMeta = new RowMeta();
      BaseSupervisedPMIData.establishOutputRowMeta( m_data.m_outputRowMeta, this, m_meta );

      m_data.m_trainingRowSet = findInputRowSet( (String) m_data.m_trainingStream.getSubject() );
      if ( m_data.m_testStream != null ) {
        m_data.m_testRowSet = findInputRowSet( (String) m_data.m_testStream.getSubject() );
      }
    }

    if ( isStopped() ) {
      return false;
    }

    if ( !m_trainingDone ) {
      Object[] row = getRowFrom( m_data.m_trainingRowSet );
      if ( row == null ) {
        m_trainingDone = true;
      }
      Object[][] outputRow = m_data.handleTrainingRow( row, m_meta, getLogChannel(), this );
      if ( outputRow != null ) {
        for ( int i = 0; i < outputRow.length; i++ ) {
          if ( outputRow[i] != null ) {
            putRow( m_data.m_outputRowMeta, outputRow[i] );
          }
        }
      }
    } else {
      // separate test set?
      if ( !m_testingDone && m_data.m_testRowSet != null ) {
        Object[] testRow = getRowFrom( m_data.m_testRowSet );
        if ( testRow == null ) {
          m_testingDone = true;
        }
        List<Object[]> outputRows = m_data.handleSeparateTestRow( testRow, m_meta, getLogChannel(), this );
        for ( Object[] r : outputRows ) {
          putRow( m_data.m_outputRowMeta, r );
        }
      }
    }

    if ( m_trainingDone && m_testingDone ) {
      m_data.cleanup();
      setOutputDone();
      return false;
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "BasePMIStep.Message.LineNumber", getLinesRead() ) );
    }

    return true;
  }
}
