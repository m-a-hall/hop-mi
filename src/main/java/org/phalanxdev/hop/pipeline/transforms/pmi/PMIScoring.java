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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.i18n.BaseMessages;
import weka.core.BatchPredictor;
import weka.core.Instances;
import weka.core.SerializedObject;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
public class PMIScoring extends BaseTransform<PMIScoringMeta, PMIScoringData> implements ITransform<PMIScoringMeta, PMIScoringData> {

  protected PMIScoringMeta m_meta;
  protected PMIScoringData m_data;

  /**
   * only used when grabbing model file names from the incoming stream
   */
  private int m_indexOfFieldToLoadFrom = -1;

  /**
   * cache for models that are loaded from files specified in incoming rows
   */
  private Map<String, PMIScoringModel> m_modelCache;

  /**
   * model filename from the last row processed (if reading model filenames from
   * a row field
   */
  private String m_lastRowModelFile = "";

  /**
   * size of the batches of rows to be scored if the model is a batch scorer
   */
  private int m_batchScoringSize = PMIScoringMeta.DEFAULT_BATCH_SCORING_SIZE;
  private List<Object[]> m_batch;

  public PMIScoring( TransformMeta transformMeta, PMIScoringMeta meta, PMIScoringData data, int copyNr, PipelineMeta transMeta,
      Pipeline trans ) {
    super( transformMeta, meta, data, copyNr, transMeta, trans );

    m_meta = meta;
    m_data = data;

  }

  /**
   * Sets the model to use from the path supplied in a user chosen field of the
   * incoming data stream. User may opt to have loaded models cached in memory.
   * User may also opt to supply a default model to be used when there is none
   * specified in the field value.
   *
   * @param row
   * @throws HopException
   */
  private void setModelFromField( Object[] row ) throws HopException {

    IRowMeta inputRowMeta = getInputRowMeta();
    String modelFileName = inputRowMeta.getString( row, m_indexOfFieldToLoadFrom );

    if ( org.apache.hop.core.util.Utils.isEmpty( modelFileName ) ) {
      // see if there is a default model to use
      PMIScoringModel defaultM = m_data.getDefaultModel();
      if ( defaultM == null ) {
        throw new HopException( BaseMessages
            .getString( PMIScoringMeta.PKG, "PMIScoring.Error.NoModelFileSpecifiedInFieldAndNoDefaultModel" ) );
      }
      logDebug( BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Debug.UsingDefaultModel" ) );
      m_data.setModel( defaultM );
      return;
    }

    String resolvedName = environmentSubstitute( modelFileName );

    if ( resolvedName.equals( m_lastRowModelFile ) ) {
      // nothing to do, just return
      return;
    }

    if ( m_meta.getCacheLoadedModels() ) {
      PMIScoringModel modelToUse = m_modelCache.get( resolvedName );
      if ( modelToUse != null ) {
        logDebug( BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Debug.FoundModelInCache" ) + " " //$NON-NLS-1$
            + modelToUse.getModel().getClass() );
        m_data.setModel( modelToUse );
        m_lastRowModelFile = resolvedName;
        return;
      }
    }

    // load the model
    logDebug(
        BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Debug.LoadingModelUsingFieldValue" ) + " " //$NON-NLS-1$
            + environmentSubstitute( modelFileName ) );
    PMIScoringModel modelToUse = setModel( modelFileName );

    if ( m_meta.getCacheLoadedModels() ) {
      m_modelCache.put( resolvedName, modelToUse );
    }
  }

  private PMIScoringModel setModel( String modelFileName ) throws HopException {

    // Load the model
    PMIScoringModel model = null;
    try {
      model = PMIScoringData.loadSerializedModel( modelFileName, getLogChannel(), this );
      m_data.setModel( model );

      if ( m_meta.getFileNameFromField() ) {
        m_lastRowModelFile = environmentSubstitute( modelFileName );
      }
    } catch ( Exception ex ) {
      throw new HopException(
          BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.ProblemDeserializingModel" ), ex );
    }
    return model;
  }

  /**
   * Process an incoming row of data.
   *
   * @return a <code>boolean</code> value
   * @throws HopException if an error occurs
   */
  @Override public boolean processRow( ) throws HopException {

    Object[] r = getRow();

    // Handle the first row
    if ( first ) {
      first = false;

      if (r == null) {
        setOutputDone();
        return false;
      }

      m_data.setOutputRowMeta( getInputRowMeta().clone() );
      if ( m_meta.getFileNameFromField() ) {
        IRowMeta inputRowMeta = getInputRowMeta();

        m_indexOfFieldToLoadFrom = inputRowMeta.indexOfValue( m_meta.getFieldNameToLoadModelFrom() );

        if ( m_indexOfFieldToLoadFrom < 0 ) {
          throw new HopException( "Unable to locate model file field " //$NON-NLS-1$
              + m_meta.getFieldNameToLoadModelFrom() + " in the incoming stream!" ); //$NON-NLS-1$
        }

        if ( !inputRowMeta.getValueMeta( m_indexOfFieldToLoadFrom ).isString() ) {
          throw new HopException( BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error." ) ); //$NON-NLS-1$
        }

        if ( m_meta.getCacheLoadedModels() ) {
          m_modelCache = new HashMap<String, PMIScoringModel>();
        }

        // set the default model
        if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getSerializedModelFileName() ) ) {
          PMIScoringModel defaultModel = setModel( m_meta.getSerializedModelFileName() );

          m_data.setDefaultModel( defaultModel );
        } else if ( m_meta.getModel() != null ) {
          try {
            SerializedObject so = new SerializedObject( m_meta.getModel() );
            PMIScoringModel defaultModel = (PMIScoringModel) so.getObject();

            m_data.setDefaultModel( defaultModel );
          } catch ( Exception ex ) {
            throw new HopException( ex );
          }
        }

        // set the main model from this row
        setModelFromField( r );
        logBasic( BaseMessages
            .getString( PMIScoringMeta.PKG, "PMIScoring.Message.SourcingModelNamesFromInputField", //$NON-NLS-1$
                m_meta.getFieldNameToLoadModelFrom() ) );
      } else if ( m_meta.getModel() == null || !org.apache.hop.core.util.Utils.isEmpty( m_meta.getSerializedModelFileName() ) ) {
        // If we don't have a model, or a file name is set, then load from file

        // Check that we have a file to try and load a classifier from
        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getSerializedModelFileName() ) ) {
          throw new HopException( BaseMessages
              .getString( PMIScoringMeta.PKG, "PMIScoring.Error.NoFilenameToLoadModelFrom" ) ); //$NON-NLS-1$
        }

        setModel( m_meta.getSerializedModelFileName() );
      } else if ( m_meta.getModel() != null ) {
        // copy the primary model over to the data class
        try {
          SerializedObject so = new SerializedObject( m_meta.getModel() );
          PMIScoringModel defaultModel = (PMIScoringModel) so.getObject();

          m_data.setModel( defaultModel );
        } catch ( Exception ex ) {
          throw new HopException( ex );
        }
      }

      // Check the input row meta data against the instances
      // header that the classifier was trained with
      try {
        Instances header = m_data.getModel().getHeader();
        m_data.mapIncomingRowMetaData( header, getInputRowMeta(), m_meta.getUpdateIncrementalModel(), log );
      } catch ( Exception ex ) {
        throw new HopException(
            BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.IncomingDataFormatDoesNotMatchModel" ),
            ex ); //$NON-NLS-1$
      }

      if ( m_meta.getEvaluateRatherThanScore() && m_data.getModel().isSupervisedLearningModel() ) {
        // check for presence of class attribute and matching type...
        try {
          int classCheck = m_data.checkClassForEval( m_data.getModel().getHeader() );
          if ( classCheck == PMIScoringData.NO_MATCH ) {
            throw new HopException( BaseMessages
                .getString( PMIScoringMeta.PKG, "PMIScoring.Error.ClassAttributeIsNotPresentForEvaluation",
                    m_data.getModel().getHeader().classAttribute().name() ) );
          } else if ( classCheck == PMIScoringData.TYPE_MISMATCH ) {
            throw new HopException( BaseMessages
                .getString( PMIScoringMeta.PKG, "PMIScoring.Error.ClassAttributeTypeMismatch",
                    m_data.getModel().getHeader().classAttribute().name() ) );
          }
          m_data.initEvaluation( m_meta );
        } catch ( Exception e ) {
          throw new HopException( e );
        }
      }

      // Determine the output format
      m_meta.getFields( m_data.getOutputRowMeta(), getTransformName(), null, null, this, null );

      if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getBatchScoringSize() ) && m_meta.getModel().isBatchPredictor() ) {
        try {
          String bss = environmentSubstitute( m_meta.getBatchScoringSize() );
          m_batchScoringSize = Integer.parseInt( bss );
        } catch ( NumberFormatException ex ) {
          String
              modelPreferred =
              environmentSubstitute( ( (BatchPredictor) m_meta.getModel().getModel() ).getBatchSize() );

          boolean sizeOk = false;
          if ( !org.apache.hop.core.util.Utils.isEmpty( modelPreferred ) ) {
            logBasic( BaseMessages
                .getString( PMIScoringMeta.PKG, "PMIScoring.Message.UnableToParseBatchScoringSize", //$NON-NLS-1$
                    modelPreferred ) );
            try {
              m_batchScoringSize = Integer.parseInt( modelPreferred );
              sizeOk = true;
            } catch ( NumberFormatException e ) {
              // ignore
            }
          }

          if ( !sizeOk ) {
            logBasic(
                BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Message.UnableToParseBatchScoringSizeDefault",
                    //$NON-NLS-1$
                    PMIScoringMeta.DEFAULT_BATCH_SCORING_SIZE ) );

            m_batchScoringSize = PMIScoringMeta.DEFAULT_BATCH_SCORING_SIZE;
          }
        }
      }

      if ( m_data.getModel().isBatchPredictor() ) {
        m_batch = new ArrayList<Object[]>();
      }
    } // end (if first)

    if ( r == null ) {
      if ( !m_meta.getEvaluateRatherThanScore() && m_data.getModel().isBatchPredictor() && !m_meta
          .getFileNameFromField() && m_batch.size() > 0 ) {
        try {
          outputBatchRows( true );
        } catch ( Exception ex ) {
          throw new HopException(
              BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.ProblemWhileGettingPredictionsForBatch" ),
              ex ); //$NON-NLS-1$
        }
      }

      if ( m_meta.getEvaluateRatherThanScore() && m_data.getModel().isSupervisedLearningModel() ) {
        // generate the output row
        try {
          if ( m_data.getModel().isBatchPredictor() ) {
            outputBatchRows( true );
          } else {
            Object[] outputRow = m_data.evaluateForRow( getInputRowMeta(), m_data.getOutputRowMeta(), null, m_meta );
            putRow( m_data.getOutputRowMeta(), outputRow );
          }
        } catch ( Exception ex ) {
          throw new HopException(
              BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.ProblemWhileGettingPredictionsForBatch" ),
              ex ); //$NON-NLS-1$
        }
      }

      // see if we have an incremental model that is to be saved somewhere.
      if ( !m_meta.getFileNameFromField() && m_meta.getUpdateIncrementalModel() ) {
        if ( !org.apache.hop.core.util.Utils.isEmpty( m_meta.getSavedModelFileName() ) ) {
          // try and save that sucker...
          try {
            String modName = environmentSubstitute( m_meta.getSavedModelFileName() );
            File updatedModelFile = null;
            if ( modName.startsWith( "file:" ) ) {
              try {
                modName = modName.replace( " ", "%20" );
                updatedModelFile = new File( new java.net.URI( modName ) );
              } catch ( Exception ex ) {
                throw new HopException(
                    BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.MalformedURIForUpdatedModelFile" ),
                    ex );
              }
            } else {
              updatedModelFile = new File( modName );
            }
            PMIScoringData.saveSerializedModel( m_data.getModel(), updatedModelFile );
          } catch ( Exception ex ) {
            throw new HopException(
                BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.ProblemSavingUpdatedModelToFile" ),
                ex ); //$NON-NLS-1$
          }
        }
      }

      if ( m_meta.getFileNameFromField() ) {
        // clear the main model
        m_data.getModel().done();
        m_data.setModel( null );
        m_data.setDefaultModel( null );
        if ( m_modelCache != null ) {
          m_modelCache.clear();
        }
      } else {
        m_data.getModel().done();
        m_data.setModel( null );
        m_data.setDefaultModel( null );
      }

      setOutputDone();
      return false;
    }

    // Make prediction for row using model
    try {
      if ( m_meta.getFileNameFromField() ) {
        setModelFromField( r );
      }

      if ( m_data.getModel().isBatchPredictor() && !m_meta.getFileNameFromField() ) {
        try {
          // add current row to batch
          m_batch.add( r );

          if ( m_batch.size() == m_batchScoringSize ) {
            outputBatchRows( false );
          }
        } catch ( Exception ex ) {
          throw new HopException(
              BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.ErrorGettingBatchPredictions" ), ex );
        }
      } else {
        Object[]
            outputRow =
            m_meta.getEvaluateRatherThanScore() ?
                m_data.evaluateForRow( getInputRowMeta(), m_data.getOutputRowMeta(), r, m_meta ) :
                m_data.generatePrediction( getInputRowMeta(), m_data.getOutputRowMeta(), r, m_meta );
        if ( outputRow != null ) {
          putRow( m_data.getOutputRowMeta(), outputRow );
        }
      }
    } catch ( Exception ex ) {
      throw new HopException(
          BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Error.UnableToMakePredictionForRow", getLinesRead() ),
          ex );
    }

    if ( log.isRowLevel() ) {
      log.logRowlevel( toString(), "Read row #" + getLinesRead() + " : " + r );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( "Linenr " + getLinesRead() );
    }
    return true;
  }

  protected void outputBatchRows( boolean finished ) throws Exception {
    // get predictions for the batch
    Object[][]
        outputRows =
        m_meta.getEvaluateRatherThanScore() ?
            m_data.evaluateForRows( getInputRowMeta(), m_data.getOutputRowMeta(), m_batch, m_meta, this ) :
            m_data.generatePredictions( getInputRowMeta(), m_data.getOutputRowMeta(), m_batch, m_meta );

    if ( finished && m_meta.getEvaluateRatherThanScore() ) {
      // make sure we get the output row that contains eval
      m_batch.clear();
      outputRows = m_data.evaluateForRows( getInputRowMeta(), m_data.getOutputRowMeta(), m_batch, m_meta, this );
    }

    if ( log.isDetailed() ) {
      logDetailed( BaseMessages.getString( PMIScoringMeta.PKG, "PMIScoring.Message.PredictingBatch" ) );
    }

    // output the rows
    if ( outputRows != null && outputRows.length > 0 ) {
      for ( Object[] row : outputRows ) {
        putRow( m_data.getOutputRowMeta(), row );
      }
    }

    // reset batch
    m_batch.clear();
  }

  /**
   * Initialize the step.
   *   * @return a <code>boolean</code> value
   */
  @Override public boolean init( ) {

    if ( super.init( ) ) {
      return true;
    }
    return false;
  }
}
