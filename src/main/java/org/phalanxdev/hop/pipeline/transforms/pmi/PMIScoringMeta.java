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

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.phalanxdev.hop.ui.pipeline.pmi.PMIScoringDialog;
import org.w3c.dom.Node;
import weka.core.Attribute;
import weka.core.Instances;
import weka.core.SerializationHelper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
@Transform( id = "PMIScoring", image = "WEKAS.svg", name = "PMI Scoring", description = "Score or evaluate a PMI model", categoryDescription = "PMI" )
public class PMIScoringMeta extends BaseTransformMeta implements ITransformMeta<PMIScoring, PMIScoringData> {

  public static Class<?> PKG = PMIScoringMeta.class;

  /**
   * Default batch scoring size
   */
  public static final int DEFAULT_BATCH_SCORING_SIZE = 100;

  /**
   * User batch scoring size
   */
  protected String m_batchScoringSize = "";

  /**
   * Use a model file specified in an incoming field
   */
  protected boolean m_fileNameFromField;

  /**
   * Whether to cache loaded models in memory (when they are being specified by
   * a field in the incoming rows
   */
  protected boolean m_cacheLoadedModels;

  /**
   * The name of the field that is being used to specify model file name/path
   */
  protected String m_fieldNameToLoadModelFrom;

  /**
   * File name of the serialized PMI (wrapped) model to load/import
   */
  protected String m_modelFileName;

  /**
   * File name to save incrementally updated model to
   */
  protected String m_savedModelFileName;

  /**
   * True if predicted probabilities are to be output (has no effect if the
   * class (target is numeric)
   */
  protected boolean m_outputProbabilities;

  /**
   * True if user has selected to update a model on the incoming data stream and
   * the model supports incremental updates and there exists a column in the
   * incoming data stream that has been matched successfully to the class
   * attribute (if one exists).
   */
  protected boolean m_updateIncrementalModel;

  /**
   * Whether to store serialized model data into the step's metadata (rather than load from filesystem)
   */
  protected boolean m_storeModelInStepMetaData;

  /**
   * Whether to perform evaluation on the incoming stream (if targets are present) rather than score the data.
   * Applies to supervised models only
   */
  protected boolean m_evaluateRatherThanScore;

  /**
   * True if information retrieval metrics are to be output when evaluating
   */
  protected boolean m_outputIRMetrics;

  /**
   * True if area under the curve metrics are to be output (these require caching predictions, so will not be suitable
   * for very large test sets)
   */
  protected boolean m_outputAUCMetrics;

  /**
   * Holds the underlying model
   */
  protected transient PMIScoringModel m_model;

  /**
   * Holds a default model. Used only when model files are sourced from a field in the incoming data rows. In this case,
   * it is the fallback model if there is no model file specified in the current incoming row. It is also necessary so
   * that getFields() can determine the full output structure
   */
  protected transient PMIScoringModel m_defaultModel;

  /**
   * Set whether to perform evaluation on the incoming stream (if targets are present) rather than score the data.
   * Applies to supervised models only
   *
   * @param evaluateRatherThanScore true to perform evaluation (and output evaluation metrics) rather than score
   *                                the incoming data
   */
  public void setEvaluateRatherThanScore( boolean evaluateRatherThanScore ) {
    m_evaluateRatherThanScore = evaluateRatherThanScore;
  }

  /**
   * Get whether to perform evaluation on the incoming stream (if targets are present) rather than score the data.
   * Applies to supervised models only
   *
   * @return true to perform evaluation (and output evaluation metrics) rather than score
   * the incoming data
   */
  public boolean getEvaluateRatherThanScore() {
    return m_evaluateRatherThanScore;
  }

  /**
   * Set whether information retrieval metrics are to be output when performing evaluation
   *
   * @param outputIRMetrics true to compute and output IR metrics
   */
  public void setOutputIRMetrics( boolean outputIRMetrics ) {
    m_outputIRMetrics = outputIRMetrics;
  }

  /**
   * Get whether information retrieval metrics are to be output when performing evaluation
   *
   * @return true to compute and output IR metrics
   */
  public boolean getOutputIRMetrics() {
    return m_outputIRMetrics;
  }

  /**
   * Set whether to output area under the curve metrics or not. Note that AUC metrics require that predictions be cached,
   * so turning this on may not be suitable for very large test sets.
   *
   * @param outputAUCMetrics true to compute and output AUC metrics
   */
  public void setOutputAUCMetrics( boolean outputAUCMetrics ) {
    m_outputAUCMetrics = outputAUCMetrics;
  }

  /**
   * Get whether to output area under the curve metrics or not. Note that AUC metrics require that predictions be cached,
   * so turning this on may not be suitable for very large test sets.
   *
   * @return true to compute and output AUC metrics
   */
  public boolean getOutputAUCMetrics() {
    return m_outputAUCMetrics;
  }

  /**
   * Set whether to store the serialized model into the step's metadata
   *
   * @param b true to serialize the model into the step metadata
   */
  public void setStoreModelInStepMetaData( boolean b ) {
    m_storeModelInStepMetaData = b;
  }

  /**
   * Get whether to store the serialized model into the step's metadata
   *
   * @return true to serialize the model into the step metadata
   */
  public boolean getStoreModelInStepMetaData() {
    return m_storeModelInStepMetaData;
  }

  /**
   * Set the batch size to use if the model is a batch scoring model
   *
   * @param size the size of the batch to use
   */
  public void setBatchScoringSize( String size ) {
    m_batchScoringSize = size;
  }

  /**
   * Get the batch size to use if the model is a batch scoring model
   *
   * @return the size of the batch to use
   */
  public String getBatchScoringSize() {
    return m_batchScoringSize;
  }

  /**
   * Set whether filename is coming from an incoming field
   *
   * @param f true if the model to use is specified via path in an incoming
   *          field value
   */
  public void setFileNameFromField( boolean f ) {
    m_fileNameFromField = f;
  }

  /**
   * Get whether filename is coming from an incoming field
   *
   * @return true if the model to use is specified via path in an incoming field
   * value
   */
  public boolean getFileNameFromField() {
    return m_fileNameFromField;
  }

  /**
   * Set whether to cache loaded models in memory
   *
   * @param l true if models are to be cached in memory
   */
  public void setCacheLoadedModels( boolean l ) {
    m_cacheLoadedModels = l;
  }

  /**
   * Get whether to cache loaded models in memory
   *
   * @return true if models are to be cached in memory
   */
  public boolean getCacheLoadedModels() {
    return m_cacheLoadedModels;
  }

  /**
   * Set the name of the incoming field that holds paths to model files
   *
   * @param fn the name of the incoming field that holds model paths
   */
  public void setFieldNameToLoadModelFrom( String fn ) {
    m_fieldNameToLoadModelFrom = fn;
  }

  /**
   * Get the name of the incoming field that holds paths to model files
   *
   * @return the name of the incoming field that holds model paths
   */
  public String getFieldNameToLoadModelFrom() {
    return m_fieldNameToLoadModelFrom;
  }

  /**
   * Set the file name of the serialized PMI model to load/import from
   *
   * @param mfile the file name
   */
  public void setSerializedModelFileName( String mfile ) {
    m_modelFileName = mfile;
  }

  /**
   * Get the filename of the serialized PMI model to load/import from
   *
   * @return the file name
   */
  public String getSerializedModelFileName() {
    return m_modelFileName;
  }

  /**
   * Set the file name that the incrementally updated model will be saved to
   * when the current stream of data terminates
   *
   * @param savedM the file name to save to
   */
  public void setSavedModelFileName( String savedM ) {
    m_savedModelFileName = savedM;
  }

  /**
   * Get the file name that the incrementally updated model will be saved to
   * when the current stream of data terminates
   *
   * @return the file name to save to
   */
  public String getSavedModelFileName() {
    return m_savedModelFileName;
  }

  /**
   * Set the PMI model
   *
   * @param model a <code>PMIScoringModel</code> that encapsulates the actual
   *              model (Classifier or Clusterer)
   */
  public void setModel( PMIScoringModel model ) {
    m_model = model;
  }

  /**
   * Get the PMI model
   *
   * @return a <code>PMIScoringModel</code> that encapsulates the actual
   * model (Classifier or Clusterer)
   */
  public PMIScoringModel getModel() {
    return m_model;
  }

  /**
   * Gets the default model (only used when model file names are being sourced
   * from a field in the incoming rows).
   *
   * @return the default model to use when there is no filename provided in the
   * incoming data row.
   */
  public PMIScoringModel getDefaultModel() {
    return m_defaultModel;
  }

  /**
   * Sets the default model (only used when model file names are being sourced
   * from a field in the incoming rows).
   *
   * @param defaultM the default model to use.
   */
  public void setDefaultModel( PMIScoringModel defaultM ) {
    m_defaultModel = defaultM;
  }

  /**
   * Set whether to predict probabilities
   *
   * @param b true if a probability distribution is to be output
   */
  public void setOutputProbabilities( boolean b ) {
    m_outputProbabilities = b;
  }

  /**
   * Get whether to predict probabilities
   *
   * @return a true if a probability distribution is to be output
   */
  public boolean getOutputProbabilities() {
    return m_outputProbabilities;
  }

  /**
   * Get whether the model is to be incrementally updated with each incoming row
   * (after making a prediction for it).
   *
   * @return a true if the model is to be updated incrementally with each
   * incoming row
   */
  public boolean getUpdateIncrementalModel() {
    return m_updateIncrementalModel;
  }

  /**
   * Set whether to update the model incrementally
   *
   * @param u true if the model should be updated with each incoming row (after
   *          predicting it)
   */
  public void setUpdateIncrementalModel( boolean u ) {
    m_updateIncrementalModel = u;
  }

  /**
   * Check for equality
   *
   * @param obj an <code>Object</code> to compare with
   * @return true if equal to the supplied object
   */
  @Override public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      PMIScoringMeta m = (PMIScoringMeta) obj;
      return ( getXML( false ) == m.getXML( false ) );
    }

    return false;
  }

  /**
   * Hash code method
   *
   * @return the hash code for this object
   */
  @Override public int hashCode() {
    return getXML( false ).hashCode();
  }

  /**
   * Clone this step's meta data
   *
   * @return the cloned meta data
   */
  @Override public Object clone() {
    PMIScoringMeta retval = (PMIScoringMeta) super.clone();

    return retval;
  }

  /**
   * Return the XML describing this (configured) step
   *
   * @return a <code>String</code> containing the XML
   */
  @Override
  public String getXml() {
    return getXML( true );
  }

  protected String getXML( boolean logging ) {
    StringBuilder retval = new StringBuilder();

    retval.append( XmlHandler.addTagValue( "output_probabilities", m_outputProbabilities ) );
    retval.append( XmlHandler.addTagValue( "update_model", m_updateIncrementalModel ) );
    retval.append( XmlHandler.addTagValue( "store_model_in_meta", m_storeModelInStepMetaData ) );

    if ( m_updateIncrementalModel ) {
      // any file name to save the changed model to?
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_savedModelFileName ) ) {
        retval.append( XmlHandler.addTagValue( "model_export_file_name", m_savedModelFileName ) );
      }
    }

    retval.append( XmlHandler.addTagValue( "file_name_from_field", m_fileNameFromField ) );
    if ( m_fileNameFromField ) {
      // any non-null field name?
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_fieldNameToLoadModelFrom ) ) {
        retval.append( XmlHandler.addTagValue( "field_name_to_load_from", m_fieldNameToLoadModelFrom ) );
        if ( logging ) {
          getLog().logDebug( BaseMessages.getString( PKG, "PMIScoringMeta.Log.ModelSourcedFromField" ) + " "
              + m_fieldNameToLoadModelFrom );
        }
      }
    }

    if ( !org.apache.hop.core.util.Utils.isEmpty( m_batchScoringSize ) ) {
      retval.append( XmlHandler.addTagValue( "batch_scoring_size", m_batchScoringSize ) );
    }

    retval.append( XmlHandler.addTagValue( "cache_loaded_models", m_cacheLoadedModels ) );

    retval.append( XmlHandler.addTagValue( "perform_evaluation", m_evaluateRatherThanScore ) );
    retval.append( XmlHandler.addTagValue( "output_ir_metrics", m_outputIRMetrics ) );
    retval.append( XmlHandler.addTagValue( "output_auc_metrics", m_outputAUCMetrics ) );

    PMIScoringModel temp = m_fileNameFromField ? m_defaultModel : m_model;
    if ( temp != null && org.apache.hop.core.util.Utils.isEmpty( getSerializedModelFileName() ) ) {
      byte[] model = serializeModelToBase64( temp );
      if ( model != null ) {
        try {
          String base64model = XmlHandler.addTagValue( "pmi_scoring_model", model );
          String modType = ( m_fileNameFromField ) ? "default" : "";
          if ( logging ) {
            getLog().logDebug( "Serializing " + modType + " model." );
            getLog().logDebug(
                BaseMessages.getString( PKG, "PMIScoringMeta.Log.SizeOfModel" ) + " " + base64model.length() );
          }

          retval.append( base64model );
        } catch ( IOException e ) {
          if ( logging ) {
            getLog().logError( BaseMessages.getString( PKG, "PMIScoringMeta.Log.Base64SerializationProblem" ) );
          }
        }
      }
    } else {
      if ( !org.apache.hop.core.util.Utils.isEmpty( m_modelFileName ) ) {

        if ( logging ) {
          logDetailed(
              BaseMessages.getString( PKG, "PMIScoringMeta.Log.ModelSourcedFromFile" ) + " " + m_modelFileName );
        }
      }

      // save the model file name
      retval.append( XmlHandler.addTagValue( "model_file_name", m_modelFileName ) );
    }

    return retval.toString();
  }

  /**
   * Loads the meta data for this (configured) step from XML.
   *
   * @param transformNode the step to load
   */
  @Override
  public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) {
    String temp = XmlHandler.getTagValue( transformNode, "file_name_from_field" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_fileNameFromField = false;
    } else {
      m_fileNameFromField = true;
    }

    if ( m_fileNameFromField ) {
      m_fieldNameToLoadModelFrom = XmlHandler.getTagValue( transformNode, "field_name_to_load_from" );
    }

    m_batchScoringSize = XmlHandler.getTagValue( transformNode, "batch_scoring_size" );

    String store = XmlHandler.getTagValue( transformNode, "store_model_in_meta" );
    if ( store != null ) {
      m_storeModelInStepMetaData = store.equalsIgnoreCase( "Y" );
    }

    String eval = XmlHandler.getTagValue( transformNode, "perform_evaluation" );
    if ( !org.apache.hop.core.util.Utils.isEmpty( eval ) ) {
      setEvaluateRatherThanScore( eval.equalsIgnoreCase( "Y" ) );
    }
    String outputIR = XmlHandler.getTagValue( transformNode, "output_ir_metrics" );
    if ( !org.apache.hop.core.util.Utils.isEmpty( outputIR ) ) {
      setOutputIRMetrics( outputIR.equalsIgnoreCase( "Y" ) );
    }
    String outputAUC = XmlHandler.getTagValue( transformNode, "output_auc_metrics" );
    if ( !org.apache.hop.core.util.Utils.isEmpty( outputAUC ) ) {
      setOutputAUCMetrics( outputAUC.equalsIgnoreCase( "Y" ) );
    }

    temp = XmlHandler.getTagValue( transformNode, "cache_loaded_models" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_cacheLoadedModels = false;
    } else {
      m_cacheLoadedModels = true;
    }

    // try and get the XML-based model
    boolean success = false;
    try {
      String base64modelXML = XmlHandler.getTagValue( transformNode, "pmi_scoring_model" );

      deSerializeBase64Model( base64modelXML );
      success = true;

      String modType = ( m_fileNameFromField ) ? "default" : "";
      logDebug( "Deserializing " + modType + " model." );

      logDebug( BaseMessages.getString( PKG, "PMIScoringMeta.Log.DeserializationSuccess" ) );
    } catch ( Exception ex ) {
      success = false;
    }

    if ( !success ) {
      // fall back and try and grab a model file name
      m_modelFileName = XmlHandler.getTagValue( transformNode, "model_file_name" );
    }

    temp = XmlHandler.getTagValue( transformNode, "output_probabilities" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_outputProbabilities = false;
    } else {
      m_outputProbabilities = true;
    }

    temp = XmlHandler.getTagValue( transformNode, "update_model" );
    if ( temp.equalsIgnoreCase( "N" ) ) {
      m_updateIncrementalModel = false;
    } else {
      m_updateIncrementalModel = true;
    }

    if ( m_updateIncrementalModel ) {
      m_savedModelFileName = XmlHandler.getTagValue( transformNode, "model_export_file_name" );
    }
  }
  
  /**
   * Generates row meta data to represent the fields output by this step
   *
   * @param row      the meta data for the output produced
   * @param origin   the name of the step to be used as the origin
   * @param info     The input rows metadata that enters the step through the
   *                 specified channels in the same order as in method getInfoSteps().
   *                 The step metadata can then choose what to do with it: ignore it or
   *                 not.
   * @param nextStep if this is a non-null value, it's the next step in the
   *                 transformation. The one who's asking, the step where the data is
   *                 targeted towards.
   * @param space    variables
   * @param metadataProvider metadata provider
   * @throws HopTransformException if an error occurs
   */
  @Override public void getFields( IRowMeta row, String origin, IRowMeta[] info, TransformMeta nextStep,
      IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    if ( m_model == null && !org.apache.hop.core.util.Utils.isEmpty( getSerializedModelFileName() ) ) {
      // see if we can load from a file.

      String modName = getSerializedModelFileName();

      try {
        if ( !PMIScoringData.modelFileExists( modName, space ) ) {
          throw new HopTransformException( BaseMessages.getString( PKG, "PMIScoring.Error.NonExistentModelFile" ) );
        }

        PMIScoringModel model = PMIScoringData.loadSerializedModel( m_modelFileName, getLog(), space );
        setModel( model );
      } catch ( Exception ex ) {
        throw new HopTransformException( BaseMessages.getString( PKG, "PMIScoring.Error.ProblemDeserializingModel" ),
            ex );
      }
    }

    if ( m_model != null ) {
      // output fields when performing evaluation rather than scoring
      if ( getEvaluateRatherThanScore() && m_model.isSupervisedLearningModel() ) {
        try {
          getFieldsEvalMode( row, space, (PMIScoringClassifier) m_model );
        } catch ( HopPluginException e ) {
          throw new HopTransformException( e );
        }
        return;
      }

      try {
        Instances header = m_model.getHeader();
        String classAttName;
        boolean supervised = m_model.isSupervisedLearningModel();

        if ( supervised ) {
          classAttName = header.classAttribute().name();

          int
              valueType =
              ( header.classAttribute().isNumeric() ) ? IValueMeta.TYPE_NUMBER : IValueMeta.TYPE_STRING;

          IValueMeta newVM = ValueMetaFactory.createValueMeta( classAttName + "_predicted", valueType );
          newVM.setOrigin( origin );
          row.addValueMeta( newVM );

          if ( m_outputProbabilities && !header.classAttribute().isNumeric() ) {
            for ( int i = 0; i < header.classAttribute().numValues(); i++ ) {
              String classVal = header.classAttribute().value( i );
              // IValueMeta
              newVM =
                  ValueMetaFactory.createValueMeta( classAttName + ":" + classVal + "_predicted_prob",
                      IValueMeta.TYPE_NUMBER );
              newVM.setOrigin( origin );
              row.addValueMeta( newVM );
            }

            // add one for the max probability too
            newVM = ValueMetaFactory.createValueMeta( classAttName + "_max_prob", IValueMeta.TYPE_NUMBER );
            newVM.setOrigin( origin );
            row.addValueMeta( newVM );
          }
        } else {
          IValueMeta
              newVM =
              ValueMetaFactory.createValueMeta( "cluster#_predicted", IValueMeta.TYPE_NUMBER );
          newVM.setOrigin( origin );
          row.addValueMeta( newVM );

          if ( m_outputProbabilities ) {
            try {
              int numClusters = ( (PMIScoringClusterer) m_model ).numberOfClusters();
              for ( int i = 0; i < numClusters; i++ ) {
                //  IValueMeta
                newVM =
                    ValueMetaFactory
                        .createValueMeta( "cluster_" + i + "_predicted_prob", IValueMeta.TYPE_NUMBER );
                newVM.setOrigin( origin );
                row.addValueMeta( newVM );
              }
              newVM = ValueMetaFactory.createValueMeta( "cluster_max_prob", IValueMeta.TYPE_NUMBER );
              newVM.setOrigin( origin );
              row.addValueMeta( newVM );
            } catch ( Exception ex ) {
              throw new HopTransformException(
                  BaseMessages.getString( PKG, "PMIScoringMeta.Error.UnableToGetNumberOfClusters" ), ex );
            }
          }
        }
      } catch ( HopPluginException e ) {
        throw new HopTransformException( e );
      }
    }
  }

  /**
   * Generates row metadata to represent evaluation-based output fields
   *
   * @param outRowMeta the output row metadata
   * @param vars       environment variables
   * @param model      the model/default model (used to obtain training data structure)
   * @throws HopPluginException if a problem occurs
   */
  protected void getFieldsEvalMode( IRowMeta outRowMeta, IVariables vars, PMIScoringClassifier model )
      throws HopPluginException {
    outRowMeta.clear();
    Instances header = model.getHeader();
    Attribute classAtt = header.classAttribute();
    boolean classIsNominal = classAtt.isNominal();

    IValueMeta vm = null;

    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.LearningSchemeName" ),
            IValueMeta.TYPE_STRING );
    outRowMeta.addValueMeta( vm );

    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.LearningSchemeOptions" ),
            IValueMeta.TYPE_STRING );
    outRowMeta.addValueMeta( vm );

    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.EvaluationMode" ),
            IValueMeta.TYPE_STRING );
    outRowMeta.addValueMeta( vm );

    vm =
        ValueMetaFactory
            .createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.UnclassifiedInstancesFieldName" ),
                IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );

    if ( classIsNominal ) {
      vm =
          ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.CorrectInstancesFieldName" ),
              IValueMeta.TYPE_NUMBER );
      outRowMeta.addValueMeta( vm );
      vm =
          ValueMetaFactory
              .createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.IncorrectInstancesFieldName" ),
                  IValueMeta.TYPE_NUMBER );
      outRowMeta.addValueMeta( vm );
      vm =
          ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.PercentCorrectFieldName" ),
              IValueMeta.TYPE_NUMBER );
      outRowMeta.addValueMeta( vm );
      vm =
          ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.PercentIncorrectFieldName" ),
              IValueMeta.TYPE_NUMBER );
      outRowMeta.addValueMeta( vm );
    }
    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.MAEFieldName" ),
            IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );
    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.RMSEFieldName" ),
            IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );
    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.RAEFieldName" ),
            IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );
    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.RRSEFieldName" ),
            IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );
    vm =
        ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.TotalNumInstancesFieldName" ),
            IValueMeta.TYPE_NUMBER );
    outRowMeta.addValueMeta( vm );

    if ( classIsNominal ) {
      vm =
          ValueMetaFactory.createValueMeta( BaseMessages.getString( PKG, "BasePMIStepData.KappaStatisticFieldName" ),
              IValueMeta.TYPE_NUMBER );
      outRowMeta.addValueMeta( vm );
    }

    if ( classIsNominal ) {
      if ( getOutputIRMetrics() ) {
        for ( int i = 0; i < classAtt.numValues(); i++ ) {
          String label = classAtt.value( i );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.TPRateFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.FPRateFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.PrecisionFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.RecallFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.FMeasureFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.MCCFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );
        }
      }

      if ( getOutputAUCMetrics() ) {
        for ( int i = 0; i < classAtt.numValues(); i++ ) {
          String label = classAtt.value( i );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.AUCFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );

          vm =
              ValueMetaFactory
                  .createValueMeta( label + "_" + BaseMessages.getString( PKG, "BasePMIStepData.PRCFieldName" ),
                      IValueMeta.TYPE_NUMBER );
          outRowMeta.addValueMeta( vm );
        }
      }
    }
  }

  protected byte[] serializeModelToBase64( PMIScoringModel model ) {
    try {
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      BufferedOutputStream bos = new BufferedOutputStream( bao );
      ObjectOutputStream oo = new ObjectOutputStream( bos );
      oo.writeObject( model );
      oo.flush();
      return bao.toByteArray();
    } catch ( Exception ex ) {
      getLog().logDebug( "Unable to serialize model to base 64!" );
    }
    return null;
  }

  protected void deSerializeBase64Model( String base64modelXML ) throws Exception {
    byte[] model = XmlHandler.stringToBinary( base64modelXML );

    // now de-serialize
    ByteArrayInputStream bis = new ByteArrayInputStream( model );
    ObjectInputStream ois = SerializationHelper.getObjectInputStream( bis );

    if ( m_fileNameFromField ) {
      m_defaultModel = (PMIScoringModel) ois.readObject();
    } else {
      m_model = (PMIScoringModel) ois.readObject();
    }
    ois.close();
  }

  @Override public void setDefault() {
    m_modelFileName = null;
    m_outputProbabilities = false;
  }

  @Override
  public ITransform createTransform( TransformMeta transformMeta, PMIScoringData pmiScoringData, int copyNr, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    return new PMIScoring( transformMeta, this, pmiScoringData, copyNr, pipelineMeta, pipeline );
  }

  @Override public PMIScoringData getTransformData() {
    return new PMIScoringData();
  }

  @Override public String getDialogClassName() {
    return PMIScoringDialog.class.getCanonicalName();
  }
}
