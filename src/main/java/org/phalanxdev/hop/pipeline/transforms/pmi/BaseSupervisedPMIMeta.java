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

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transform.stream.Stream;
import org.apache.hop.pipeline.transform.stream.StreamIcon;
import org.phalanxdev.hop.utils.ArffMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.phalanxdev.mi.Evaluator;
import org.phalanxdev.mi.engines.WekaEngine;
import org.phalanxdev.hop.ui.pipeline.pmi.BaseSupervisedPMIDialog;

import org.w3c.dom.Node;
import weka.core.Utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for PMI transform metadata. Handles all the configuration required for an arbitrary PMI classifier/regressor.
 * Subclasses need only provide a no-args constructor that calls s{@code setSchemeName()} in order to set the internal
 * name for a scheme that PMI supports ({@see Scheme}) for a list of schemes that are supported. Note that a given engine
 * might or might not support a specific scheme.
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
public abstract class BaseSupervisedPMIMeta extends BaseTransformMeta<BaseSupervisedPMI, BaseSupervisedPMIData>{

  public static Class<?> PKG = BaseSupervisedPMIMeta.class;

  protected static final String ENGINE_NAME_TAG = "engine_name";
  protected static final String SCHEME_NAME_TAG = "scheme_name";
  protected static final String SCHEME_OPTS_TAG = "scheme_options";
  protected static final String ROW_HANDLING_MODE = "row_handling_mode";
  protected static final String BATCH_SIZE_TAG = "batch_size";
  protected static final String USE_RESERVOIR_SAMPLING_TAG = "use_reservoir_sampling";
  protected static final String RESERVOIR_SAMPLING_SIZE_TAG = "reservoir_size";
  protected static final String RESERVOIR_SAMPLING_RANDOM_SEED_TAG = "reservoir_seed";
  protected static final String STRATIFICATION_FIELD_NAME_TAG = "stratification_field_name";
  protected static final String INCOMING_FIELD_META_TAG = "incoming_field_meta";
  protected static final String CLASS_FIELD_TAG = "class_attribute";
  protected static final String TRAINING_STEP_INPUT_NAME_TAG = "training_step_input_name";
  protected static final String TEST_STEP_INPUT_NAME_TAG = "test_step_input_name";
  protected static final String SAMPLING_CONFIGS_TAG = "sampling_configs";
  protected static final String PREPROCESSING_CONFIGS_TAG = "preprocessing_configs";
  protected static final String SINGLE_SAMPLING_CONFIG_TAG = "sampling_config";
  protected static final String SINGLE_PREPROCESSING_CONFIG_TAG = "preprocessing_config";
  protected static final String EVAL_MODE_TAG = "evaluation_mode";
  protected static final String SPLIT_PERCENTAGE_TAG = "split_percentage";
  protected static final String X_VAL_FOLDS_TAG = "x_val_folds";
  protected static final String RANDOM_SEED_TAG = "random_seed";
  protected static final String MODEL_OUTPUT_DIRECTORY_TAG = "model_output_path";
  protected static final String MODEL_FILE_NAME_TAG = "model_file_name";
  protected static final String RESUMABLE_MODEL_LOAD_PATH_TAG = "resumable_model_load_path";
  protected static final String OUTPUT_AUC_METRICS_TAG = "output_auc_metrics";
  protected static final String OUTPUT_IR_METRICS_TAG = "output_ir_metrics";
  protected static final String INCREMENTAL_TRAININ_INITIAL_ROW_CACHE_SIZE_TAG = "incremental_initial_cache";

  /**
   * Default row handling strategy
   */
  protected static final String
      DEFAULT_ROWS_TO_PROCESS =
      BaseMessages.getString( PKG, "BasePMIStepDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label" );

  /**
   * Engine to use
   */
  protected String m_engineName = WekaEngine.ENGINE_NAME;

  /**
   * The name of the predictive scheme to use
   */
  protected String m_schemeName = "";

  /**
   * The command-line options of the underlying predictive scheme
   */
  protected String m_underlyingSchemeOptions = "";

  protected String m_rowsToProcess = DEFAULT_ROWS_TO_PROCESS;

  protected String m_stratificationFieldName = "";

  /**
   * Batch size to use when row handling is set to batch mode
   */
  protected String m_batchSize = "";

  /**
   * True if reservoir sampling is to be performed
   */
  protected boolean m_useReservoirSampling;

  /**
   * Random seed for reservoir sampling
   */
  protected String m_randomSeedReservoirSampling = "1";

  /**
   * Size of the reservoir if doing reservoir sampling
   */
  protected String m_reservoirSize = "";

  /**
   * Info on incoming fields and how they should be treated for the modeling process
   */
  protected List<ArffMeta> m_fieldMeta = new ArrayList<>();

  /**
   * The name of the step providing training data (this will be ignored if there is only one incoming info stream)
   */
  protected String m_trainingStepInputName = "";

  /**
   * The name of the step providing separate test set data (this will be ignored if there is only one incoming info stream)
   */
  protected String m_testingStepInputName = "";

  /**
   * Configs for sampling
   */
  protected Map<String, String> m_samplingConfigs = new LinkedHashMap<>();

  /**
   * Configs for preprocessing
   */
  protected Map<String, String> m_preprocessingConfigs = new LinkedHashMap<>();

  /**
   * The evaluation to perform
   */
  protected Evaluator.EvalMode m_evalMode = Evaluator.EvalMode.NONE;

  /**
   * Size of the training split (%) if doing a percentage split evaluation
   */
  protected String m_percentageSplit = "66";

  /**
   * Number of folds to use if doing a cross-validation evaluation
   */
  protected String m_xValFolds = "10";

  /**
   * Random number seed to use for splitting the data in percentage split or cross-validation
   */
  protected String m_randomSeed = "1";

  /**
   * True to output the area under ROC and PRC curves when performing evaluation. This requires storing predictions, so
   * consumes more memory. It also requires that class labels are pre-specified by the user (so that output row metadata
   * can be generated prior to execution).
   */
  protected boolean m_outputAUCMetrics;

  /**
   * True to output IR metrics when performing evaluation. This requires storing predictions, so
   * consumes more memory. It also requires that class labels are pre-specified by the user (so that output row metadata
   * can be generated prior to execution).
   */
  protected boolean m_outputIRMetrics;

  /**
   * The name of the class attribute
   */
  protected String m_classField = "";

  /**
   * Directory to save the final model to
   */
  protected String m_modelOutputPath = "";

  /**
   * File name for the model
   */
  protected String m_modelFileName = "";

  /**
   * The number of rows to cache from the start of the incoming training stream when learning an incremental model. This will only be used if 1) the
   * underlying scheme supports incremental training and 2) the header for the data stream cannot be fully determined from the incoming row metadata
   * (i.e. there are string fields that will be converted to nominal attributes and the user has not specified the legal values apriori).
   */
  protected String m_incrementalInitialCache = "100";

  /**
   * If a scheme is resumable (implements IterativeClassifier), then this field can be used to specify a model to load
   * for continued training (eval modes NONE or SEPARATE_TEST only).
   */
  protected String m_resumableModelLoadPath = "";

  // --------- row handling --------------

  /**
   * Set the row handling strategy
   *
   * @param rowsToProcess the row handling strategy
   */
  public void setRowsToProcess( String rowsToProcess ) {
    m_rowsToProcess = rowsToProcess;
  }

  /**
   * Get the row handling strategy
   *
   * @return the row handling strategy
   */
  public String getRowsToProcess() {
    return m_rowsToProcess;
  }

  /**
   * If row handling mode is "stratified", then set the name of the field to
   * stratify on
   *
   * @param fieldName the name of the field to stratify on, if row handling mode is "stratified"
   */
  public void setStratificationFieldName( String fieldName ) {
    m_stratificationFieldName = fieldName;
  }

  /**
   * If row handling mode is "stratified", then get the name of the field to
   * stratify on
   *
   * @return the name of the field to stratify on, if row handling mode is "stratified"
   */
  public String getStratificationFieldName() {
    return m_stratificationFieldName;
  }

  /**
   * Set the size of the batch to use when row handling mode is set to "batch"
   *
   * @param batchSize the size of the batch to use
   */
  public void setBatchSize( String batchSize ) {
    m_batchSize = batchSize;
  }

  /**
   * Get the size of the batch to use when row handling mode is set to "batch"
   *
   * @return the size of the batch to use
   */
  public String getBatchSize() {
    return m_batchSize;
  }

  /**
   * Set whether to apply reservoir sampling. If true, then reservoir sampling is done
   * before any batch sampling/class balancing
   *
   * @param useReservoirSampling true if reservoir sampling is to be used
   */
  public void setUseReservoirSampling( boolean useReservoirSampling ) {
    m_useReservoirSampling = useReservoirSampling;
  }

  /**
   * Get whether to apply reservoir sampling. If true, then reservoir sampling is done
   * before any batch sampling/class balancing
   *
   * @return true if reservoir sampling is to be used
   */
  public boolean getUseReservoirSampling() {
    return m_useReservoirSampling;
  }

  /**
   * Set the seed used for reservoir sampling
   *
   * @param randomSeed the seed used for reservoir sampling
   */
  public void setRandomSeedReservoirSampling( String randomSeed ) {
    m_randomSeedReservoirSampling = randomSeed;
  }

  /**
   * Get the seed used for reservoir sampling
   *
   * @return the seed used for reservoir sampling
   */
  public String getRandomSeedReservoirSampling() {
    return m_randomSeedReservoirSampling;
  }

  /**
   * Set the size of the reservoir to use, if doing reservoir sampling
   *
   * @param reservoirSize the size of the reservoir to use
   */
  public void setReservoirSize( String reservoirSize ) {
    m_reservoirSize = reservoirSize;
  }

  /**
   * Get the size of the reservoir to use, if doing reservoir sampling
   *
   * @return the size of the reservoir to use
   */
  public String getReservoirSize() {
    return m_reservoirSize;
  }

  /**
   * Set the name of the step that is providing training data. This value can/will be safely ignored
   * in the case where there is only one step connected (assumed to be training data).
   *
   * @param name the name of the step providing training data
   */
  public void setTrainingStepInputName( String name ) {
    m_trainingStepInputName = name;
  }

  /**
   * Get the name of the step that is providing training data. This value can/will be safely ignored
   * in the case where there is only one step connected (assumed to be training data).
   *
   * @return the name of the step providing training data
   */
  public String getTrainingStepInputName() {
    return m_trainingStepInputName;
  }

  /**
   * Set the name of the step that is providing testing data. This value can/will be safely ignored
   * in the case where there is only one step connected (assumed to be training data).
   *
   * @param name the name of the step providing testing data
   */
  public void setTestingStepInputName( String name ) {
    m_testingStepInputName = name;
  }

  /**
   * Get the name of the step that is providing testing data. This value can/will be safely ignored
   * in the case where there is only one step connected (assumed to be training data).
   *
   * @return the name of the step providing testing data
   */
  public String getTestingStepInputName() {
    return m_testingStepInputName;
  }

  /**
   * Set the sampling configs to use for the scheme. These relate to batch
   * sampling using Weka's libraries.
   *
   * @param samplingConfigs the sampling configs to use for the scheme
   */
  public void setSamplingConfigs( Map<String, String> samplingConfigs ) {
    m_samplingConfigs = samplingConfigs;
  }

  /**
   * Get the sampling configs to use for the scheme
   *
   * @return the sampling configs to use for the scheme
   */
  public Map<String, String> getSamplingConfigs() {
    return m_samplingConfigs;
  }

  // --------- engine configuration --------------

  /**
   * Set the engine to use
   *
   * @param engineName the name of the engine to use
   */
  public void setEngineName( String engineName ) {
    m_engineName = engineName;
  }

  /**
   * Get the engine to use
   *
   * @return the name of the engine to use
   */
  public String getEngineName() {
    return m_engineName;
  }

  // --------- field configuration ------------

  /**
   * Set the field metadata info to use
   *
   * @param fieldMetadata the field metadata
   */
  public void setFieldMetadata( List<ArffMeta> fieldMetadata ) {
    m_fieldMeta = fieldMetadata;
  }

  /**
   * Get the field metadata info to use
   *
   * @return the field metadata
   */
  public List<ArffMeta> getFieldMetadata() {
    return m_fieldMeta;
  }

  /**
   * Set the incoming field to be considered as the class attribute
   *
   * @param classAttribute the name of the incoming field to use for the class
   */
  public void setClassField( String classAttribute ) {
    m_classField = classAttribute;
  }

  /**
   * Get the incoming field to be considered as the class attribute
   *
   * @return the name of the incoming field to use for the class
   */
  public String getClassField() {
    return m_classField;
  }

  // Preprocessing is (at least initially) going to be field manupulation -
  // e.g. string fields -> numeric term/freq vectors; collapsing high arity
  // nominal fields

  /**
   * Set the preprocessing configs to use with the scheme
   *
   * @param preprocessingConfigs the preprocessing configs to use
   */
  public void setPreprocessingConfigs( Map<String, String> preprocessingConfigs ) {
    m_preprocessingConfigs = preprocessingConfigs;
  }

  /**
   * Get the preprocessing configs to use with the scheme
   *
   * @return the preprocessing configs to use
   */
  public Map<String, String> getPreprocessingConfigs() {
    return m_preprocessingConfigs;
  }

  // --------- scheme configuration -----------

  /**
   * Set the name of the scheme to use - this is "fixed" by the individual steps that
   * extend BasePMIStep for various schemes
   *
   * @param schemeName the name of the scheme to use
   */
  protected void setSchemeName( String schemeName ) {
    m_schemeName = schemeName;
  }

  /**
   * Get the name of the scheme to use
   *
   * @return the name of the scheme to use
   */
  public String getSchemeName() {
    return m_schemeName;
  }

  /**
   * Set the command-line options for the underlying scheme
   *
   * @param commandLineOptions the command-line options to use
   */
  public void setSchemeCommandLineOptions( String commandLineOptions ) {
    m_underlyingSchemeOptions = commandLineOptions;
  }

  /**
   * Get the command-line options for the underlying scheme
   *
   * @return the command-line options to use
   */
  public String getSchemeCommandLineOptions() {
    return m_underlyingSchemeOptions;
  }

  // ------------- model output opts -----------------

  /**
   * Set the path (directory) to save the model to
   *
   * @param path the path to save the model to
   */
  public void setModelOutputPath( String path ) {
    m_modelOutputPath = path;
  }

  /**
   * Get the path (directory) to save the model to
   *
   * @return the path to save the model to
   */
  public String getModelOutputPath() {
    return m_modelOutputPath;
  }

  /**
   * Set the file name to store the model as
   *
   * @param fileName the file name to store as
   */
  public void setModelFileName( String fileName ) {
    m_modelFileName = fileName;
  }

  /**
   * Get the file name to store the model as
   *
   * @return the file name to store as
   */
  public String getModelFileName() {
    return m_modelFileName;
  }

  /**
   * Set a path to a searialize resumable model to load (and continue training)
   *
   * @param resumableModelPath the path to the resumable model
   */
  public void setResumableModelPath( String resumableModelPath ) {
    m_resumableModelLoadPath = resumableModelPath;
  }

  /**
   * Get the path to a searialize resumable model to load (and continue training)
   *
   * @return the path to the resumable model
   */
  public String getResumableModelPath() {
    return m_resumableModelLoadPath;
  }

  // ------------- evaluation opts -------------------

  /**
   * Set the evaluation mode to use
   *
   * @param mode the evaluation mode to use
   */
  public void setEvalMode( Evaluator.EvalMode mode ) {
    m_evalMode = mode;
  }

  /**
   * Get the evaluation mode to use
   *
   * @return the evaluation mode to use
   */
  public Evaluator.EvalMode getEvalMode() {
    return m_evalMode;
  }

  /**
   * Set the split percentage, if using percentage split eval.
   *
   * @param split the split percentage (1 - 99) to use for training
   */
  public void setPercentageSplit( String split ) {
    m_percentageSplit = split;
  }

  /**
   * Get the split percentage, if using percentage split eval.
   *
   * @return the split percentage (1 - 99) to use for training
   */
  public String getPercentageSplit() {
    return m_percentageSplit;
  }

  /**
   * Set the number of cross-validation folds to use ( >= 2)
   *
   * @param folds the number of cross-validation folds to use
   */
  public void setXValFolds( String folds ) {
    m_xValFolds = folds;
  }

  /**
   * Get the number of cross-validation folds to use ( >= 2)
   *
   * @return the number of cross-validation folds to use
   */
  public String getXValFolds() {
    return m_xValFolds;
  }

  /**
   * Set the random seed used when splitting data via percentage split or into folds
   *
   * @param randomSeed the seed to use in the random number generator
   */
  public void setRandomSeed( String randomSeed ) {
    m_randomSeed = randomSeed;
  }

  /**
   * Get the random seed used when splitting data via percentage split or into folds
   *
   * @return the seed to use in the random number generator
   */
  public String getRandomSeed() {
    return m_randomSeed;
  }

  /**
   * Set whether to output area under the curve metrics when evaluating
   *
   * @param outputAUCMetrics true to output area under the curve metrics
   */
  public void setOutputAUCMetrics( boolean outputAUCMetrics ) {
    m_outputAUCMetrics = outputAUCMetrics;
  }

  /**
   * Get whether to output area under the curve metrics when evaluating
   *
   * @return true to output area under the curve metrics
   */
  public boolean getOutputAUCMetrics() {
    return m_outputAUCMetrics;
  }

  /**
   * Set whether to output IR retrieval metrics when evaluating
   *
   * @param outputIRMetrics true to output IR metrics
   */
  public void setOutputIRMetrics( boolean outputIRMetrics ) {
    m_outputIRMetrics = outputIRMetrics;
  }

  /**
   * Get whether to output IR retrieval metrics when evaluating
   *
   * @return true to output IR metrics
   */
  public boolean getOutputIRMetrics() {
    return m_outputIRMetrics;
  }

  // -- incremental schemes ---

  /**
   * Set the number of rows to cache from the start of the incoming training stream when learning an incremental model.
   * This will only be used if 1) the underlying scheme supports incremental training and 2) the header for the data
   * stream cannot be fully determined from the incoming row metadata
   * (i.e. there are string fields that will be converted to nominal attributes and the user has not specified the
   * legal values apriori).
   *
   * @param rowCacheSize the number of rows to cache (if necessary) from the start of the stream
   */
  public void setInitialRowCacheForNominalValDetermination( String rowCacheSize ) {
    m_incrementalInitialCache = rowCacheSize;
  }

  /**
   * Get the number of rows to cache from the start of the incoming training stream when learning an incremental model.
   * This will only be used if 1) the underlying scheme supports incremental training and 2) the header for the data
   * stream cannot be fully determined from the incoming row metadata
   * (i.e. there are string fields that will be converted to nominal attributes and the user has not specified the
   * legal values apriori).
   *
   * @return the number of rows to cache (if necessary) from the start of the stream
   */
  public String getInitialRowCacheForNominalValDetermination() {
    return m_incrementalInitialCache;
  }

  @Override public String getXml() {
    StringBuilder buff = new StringBuilder();

    buff.append( XmlHandler.addTagValue( ENGINE_NAME_TAG, getEngineName() ) );
    buff.append( XmlHandler.addTagValue( SCHEME_NAME_TAG, getSchemeName() ) );
    buff.append( XmlHandler.addTagValue( SCHEME_OPTS_TAG, getSchemeCommandLineOptions() ) );
    buff.append( XmlHandler.addTagValue( ROW_HANDLING_MODE, getRowsToProcess() ) );
    buff.append( XmlHandler.addTagValue( BATCH_SIZE_TAG, getBatchSize() ) );
    buff.append( XmlHandler.addTagValue( USE_RESERVOIR_SAMPLING_TAG, getUseReservoirSampling() ) );
    buff.append( XmlHandler.addTagValue( RESERVOIR_SAMPLING_RANDOM_SEED_TAG, getRandomSeedReservoirSampling() ) );
    buff.append( XmlHandler.addTagValue( RESERVOIR_SAMPLING_SIZE_TAG, getReservoirSize() ) );
    buff.append( XmlHandler.addTagValue( STRATIFICATION_FIELD_NAME_TAG, getStratificationFieldName() ) );
    buff.append( XmlHandler.addTagValue( CLASS_FIELD_TAG, getClassField() ) );
    buff.append( XmlHandler.addTagValue( TRAINING_STEP_INPUT_NAME_TAG, getTrainingStepInputName() ) );
    buff.append( XmlHandler.addTagValue( TEST_STEP_INPUT_NAME_TAG, getTestingStepInputName() ) );
    buff.append( XmlHandler.addTagValue( EVAL_MODE_TAG, getEvalMode().toString() ) );
    buff.append( XmlHandler.addTagValue( SPLIT_PERCENTAGE_TAG, getPercentageSplit() ) );
    buff.append( XmlHandler.addTagValue( X_VAL_FOLDS_TAG, getXValFolds() ) );
    buff.append( XmlHandler.addTagValue( RANDOM_SEED_TAG, getRandomSeed() ) );
    buff.append( XmlHandler.addTagValue( MODEL_OUTPUT_DIRECTORY_TAG, getModelOutputPath() ) );
    buff.append( XmlHandler.addTagValue( MODEL_FILE_NAME_TAG, getModelFileName() ) );
    buff.append( XmlHandler.addTagValue( RESUMABLE_MODEL_LOAD_PATH_TAG, getResumableModelPath() ) );
    buff.append( XmlHandler.addTagValue( OUTPUT_AUC_METRICS_TAG, getOutputAUCMetrics() ) );
    buff.append( XmlHandler.addTagValue( OUTPUT_IR_METRICS_TAG, getOutputIRMetrics() ) );
    buff.append( XmlHandler.addTagValue( INCREMENTAL_TRAININ_INITIAL_ROW_CACHE_SIZE_TAG,
        getInitialRowCacheForNominalValDetermination() ) );

    // incoming field metadata
    if ( m_fieldMeta.size() > 0 ) {
      buff.append( XmlHandler.openTag( INCOMING_FIELD_META_TAG ) ).append( Const.CR );
      for ( ArffMeta arffMeta : m_fieldMeta ) {
        buff.append( "  " ).append( arffMeta.getXML() ).append( Const.CR );
      }
      buff.append( XmlHandler.closeTag( INCOMING_FIELD_META_TAG ) ).append( Const.CR );
    }

    // sampling configs
    if ( m_samplingConfigs.size() > 0 ) {
      buff.append( XmlHandler.openTag( SAMPLING_CONFIGS_TAG ) ).append( Const.CR );
      int i = 0;
      for ( Map.Entry<String, String> e : m_samplingConfigs.entrySet() ) {
        buff.append( "  " )
            .append( XmlHandler.addTagValue( SINGLE_SAMPLING_CONFIG_TAG + i++, e.getKey() + " " + e.getValue() ) );
      }
      buff.append( XmlHandler.closeTag( SAMPLING_CONFIGS_TAG ) ).append( Const.CR );
    }

    // preprocessing configs
    if ( m_preprocessingConfigs.size() > 0 ) {
      buff.append( XmlHandler.openTag( PREPROCESSING_CONFIGS_TAG ) ).append( Const.CR );
      int i = 0;
      for ( Map.Entry<String, String> e : m_preprocessingConfigs.entrySet() ) {
        buff.append( "  " )
            .append( XmlHandler.addTagValue( SINGLE_PREPROCESSING_CONFIG_TAG + i++, e.getKey() + " " + e.getValue() ) );
      }
      buff.append( XmlHandler.closeTag( PREPROCESSING_CONFIGS_TAG ) ).append( Const.CR );
    }

    return buff.toString();
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    setEngineName( XmlHandler.getTagValue( transformNode, ENGINE_NAME_TAG ) );
    setSchemeName( XmlHandler.getTagValue( transformNode, SCHEME_NAME_TAG ) );
    String schemeOpts = XmlHandler.getTagValue( transformNode, SCHEME_OPTS_TAG );
    setSchemeCommandLineOptions( schemeOpts == null ? "" : schemeOpts );
    String rowHandling = XmlHandler.getTagValue( transformNode, ROW_HANDLING_MODE );
    setRowsToProcess( rowHandling == null ? "" : rowHandling );
    String batchSize = XmlHandler.getTagValue( transformNode, BATCH_SIZE_TAG );
    setBatchSize( batchSize == null ? "" : batchSize );
    setUseReservoirSampling(
        XmlHandler.getTagValue( transformNode, USE_RESERVOIR_SAMPLING_TAG ).equalsIgnoreCase( "Y" ) );
    String reservoirSize = XmlHandler.getTagValue( transformNode, RESERVOIR_SAMPLING_SIZE_TAG );
    setReservoirSize( reservoirSize == null ? "" : reservoirSize );
    String reservoirSeed = XmlHandler.getTagValue( transformNode, RESERVOIR_SAMPLING_RANDOM_SEED_TAG );
    setRandomSeedReservoirSampling( reservoirSeed == null ? "1" : reservoirSeed );
    String stratificationField = XmlHandler.getTagValue( transformNode, STRATIFICATION_FIELD_NAME_TAG );
    setStratificationFieldName( stratificationField == null ? "" : stratificationField );
    String classField = XmlHandler.getTagValue( transformNode, CLASS_FIELD_TAG );
    setClassField( classField == null ? "" : classField );
    String trainingStepInput = XmlHandler.getTagValue( transformNode, TRAINING_STEP_INPUT_NAME_TAG );
    setTrainingStepInputName( trainingStepInput == null ? "" : trainingStepInput );
    String testStepInput = XmlHandler.getTagValue( transformNode, TEST_STEP_INPUT_NAME_TAG );
    setTestingStepInputName( testStepInput == null ? "" : testStepInput );
    String evalMode = XmlHandler.getTagValue( transformNode, EVAL_MODE_TAG );
    for ( Evaluator.EvalMode evalV : Evaluator.EvalMode.values() ) {
      if ( evalV.toString().equalsIgnoreCase( evalMode ) ) {
        setEvalMode( evalV );
        break;
      }
    }
    String splitPercentage = XmlHandler.getTagValue( transformNode, SPLIT_PERCENTAGE_TAG );
    setPercentageSplit( splitPercentage == null ? "" : splitPercentage );
    String xValFolds = XmlHandler.getTagValue( transformNode, X_VAL_FOLDS_TAG );
    setXValFolds( xValFolds == null ? "" : xValFolds );
    String randomSeed = XmlHandler.getTagValue( transformNode, RANDOM_SEED_TAG );
    setRandomSeed( randomSeed == null ? "" : randomSeed );
    String modelOutputPath = XmlHandler.getTagValue( transformNode, MODEL_OUTPUT_DIRECTORY_TAG );
    setModelOutputPath( modelOutputPath == null ? "" : modelOutputPath );
    String modelFileName = XmlHandler.getTagValue( transformNode, MODEL_FILE_NAME_TAG );
    setModelFileName( modelFileName == null ? "" : modelFileName );
    String resumeModelLoadPath = XmlHandler.getTagValue( transformNode, RESUMABLE_MODEL_LOAD_PATH_TAG );
    setResumableModelPath( resumeModelLoadPath == null ? "" : resumeModelLoadPath );
    setOutputAUCMetrics( XmlHandler.getTagValue( transformNode, OUTPUT_AUC_METRICS_TAG ).equalsIgnoreCase( "Y" ) );
    setOutputIRMetrics( XmlHandler.getTagValue( transformNode, OUTPUT_IR_METRICS_TAG ).equalsIgnoreCase( "Y" ) );
    String incrementalCache = XmlHandler.getTagValue( transformNode, INCREMENTAL_TRAININ_INITIAL_ROW_CACHE_SIZE_TAG );
    setInitialRowCacheForNominalValDetermination( incrementalCache == null ? "100" : incrementalCache );

    // incoming field metadata
    Node fields = XmlHandler.getSubNode( transformNode, INCOMING_FIELD_META_TAG );
    if ( fields != null ) {
      int nrFields = XmlHandler.countNodes( fields, ArffMeta.XML_TAG );
      m_fieldMeta.clear();
      for ( int i = 0; i < nrFields; i++ ) {
        m_fieldMeta.add( new ArffMeta( XmlHandler.getSubNodeByNr( fields, ArffMeta.XML_TAG, i ) ) );
      }
    }

    try {
      // sampling configs
      fields = XmlHandler.getSubNode( transformNode, SAMPLING_CONFIGS_TAG );
      if ( fields != null ) {
        int i = 0;
        Node confNode = null;
        while ( ( confNode = XmlHandler.getSubNode( fields, SINGLE_SAMPLING_CONFIG_TAG + i ) ) != null ) {
          String config = XmlHandler.getNodeValue( confNode );
          String[] parts = Utils.splitOptions( config );
          String samplerClass = parts[0].trim();
          parts[0] = "";
          m_samplingConfigs.put( samplerClass, Utils.joinOptions( parts ) );
          i++;
        }
      }

      // preprocessing configs
      fields = XmlHandler.getSubNode( transformNode, PREPROCESSING_CONFIGS_TAG );
      if ( fields != null ) {
        int i = 0;
        Node confNode = null;
        while ( ( confNode = XmlHandler.getSubNode( fields, SINGLE_PREPROCESSING_CONFIG_TAG + i ) ) != null ) {
          String config = XmlHandler.getNodeValue( confNode );
          String[] parts = Utils.splitOptions( config );
          String preprocessorClass = parts[0].trim();
          parts[0] = "";
          m_preprocessingConfigs.put( preprocessorClass, Utils.joinOptions( parts ) );
          i++;
        }
      }
    } catch ( Exception e ) {
      throw new HopXmlException( e );
    }

    List<IStream> infoStreams = getStepIOMeta().getInfoStreams();
    if ( !org.apache.hop.core.util.Utils.isEmpty( getTrainingStepInputName() ) && infoStreams.size() > 0 ) {
      infoStreams.get( 0 ).setSubject( getTrainingStepInputName() );
    }
    if ( !org.apache.hop.core.util.Utils.isEmpty( getTestingStepInputName() ) && infoStreams.size() > 1 ) {
      infoStreams.get( 1 ).setSubject( getTestingStepInputName() );
    }
  }

  @Override
  public void getFields( IRowMeta rowMeta, String name, IRowMeta[] info, TransformMeta nextTransform, IVariables space,
      IHopMetadataProvider metadataProvider ) throws HopTransformException {
    try {
      BaseSupervisedPMIData.establishOutputRowMeta( rowMeta, space, this );
    } catch ( HopPluginException e ) {
      throw new HopTransformException( e );
    }
  }

  @Override public void setDefault() {
    m_rowsToProcess = DEFAULT_ROWS_TO_PROCESS;
    m_engineName = WekaEngine.ENGINE_NAME;
  }

  @Override public String getDialogClassName() {
    return BaseSupervisedPMIDialog.class.getCanonicalName();
  }

  public void clearStepIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta();
    ( (TransformIOMeta) ioMeta ).clearStreams();
  }

  @Override public void resetTransformIoMeta() {
    // Don't reset!
  }

  @Override public void searchInfoAndTargetTransforms( List<TransformMeta> steps ) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();

    for ( IStream stream : targetStreams ) {
      stream.setTransformMeta( TransformMeta.findTransform( steps, (String) stream.getSubject() ) );
    }
  }

  public ITransformIOMeta getStepIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta();

    if ( ioMeta.getTargetStreams().isEmpty() ) {
      // ioMeta = new TransformIOMeta( true, true, false, true, false, false );
      ( (TransformIOMeta) ioMeta ).setInputAcceptor( true );
      ( (TransformIOMeta) ioMeta ).setOutputProducer( true );
      ( (TransformIOMeta) ioMeta ).setInputOptional( true );
      ( (TransformIOMeta) ioMeta ).setSortedDataRequired( false );
      ioMeta.setInputDynamic( false );
      ioMeta.setOutputDynamic( false );

      ioMeta.addStream( new Stream( IStream.StreamType.INFO, null, "Training stream", StreamIcon.INFO, null ) );
      // if ( getEvalMode() == Evaluator.EvalMode.SEPARATE_TEST_SET ) {
      ioMeta.addStream( new Stream( IStream.StreamType.INFO, null, "Test stream", StreamIcon.INFO, null ) );
      // }

      // setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  /* public void check( List<ICheckResult> remarks, PipelineMeta transmeta, TransformMeta transformMeta,
      IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
      IHopMetadataProvider metadataProvider ) {
    // TODO
  } */
}
