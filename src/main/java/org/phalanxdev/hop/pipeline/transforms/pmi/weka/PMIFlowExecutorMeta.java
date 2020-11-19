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

package org.phalanxdev.hop.pipeline.transforms.pmi.weka;

import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.phalanxdev.hop.utils.ArffMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.phalanxdev.hop.ui.pipeline.pmi.weka.PMIFlowExecutorDialog;
import org.w3c.dom.Node;
import weka.core.Attribute;
import weka.core.Environment;
import weka.core.Instances;
import weka.core.WekaException;
import weka.knowledgeflow.Flow;
import weka.knowledgeflow.StepManager;
import weka.knowledgeflow.StepManagerImpl;

/**
 * Contains the metadata for the PMI Flow Executor step
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
@Transform( id = "PMIFlowExecutor", image = "WEKAS.svg", name = "PMI Flow Executor", description =
    "Executes a WEKA Knowledge Flow data "
        + "mining process", documentationUrl = "http://wiki.pentaho.com/display/EAI/Knowledge+Flow", categoryDescription = "PMI" )
public class PMIFlowExecutorMeta extends BaseTransformMeta implements
    ITransformMeta<PMIFlowExecutor, PMIFlowExecutorData> {

  public static Class<?> PKG = PMIFlowExecutorMeta.class;

  /**
   * XML tag for the KF step
   */
  public static final String XML_TAG = "weka_kf"; //$NON-NLS-1$

  /**
   * Meta data for the ARFF instances input to the inject step
   */
  protected ArffMeta[] m_injectFields;

  /**
   * File name of the serialized Weka knowledge flow load/import
   */
  private String m_flowFileName;

  /**
   * Whether to store the flow in the step meta data or not
   */
  private boolean m_storeFlowInStepMetaData;

  /**
   * Name of the KnowledgeFlow step to inject data into
   */
  private String m_injectStepName;

  /**
   * Name of the event to generate for the inject step
   */
  private String m_injectEventName;

  /**
   * Name of the KnowledgeFlow step to listen for output from
   */
  private String m_outputStepName;

  /**
   * Name of the event to listen for from the output step
   */
  private String m_outputConnectionName;

  /**
   * Pass rows through rather than listening for output
   */
  private boolean m_passRowsThrough = false;

  /**
   * Holds the actual knowledge flow to run
   */
  private String m_flow;

  /**
   * Relation name for sampled data set
   */
  private String m_sampleRelationName = "Sampled rows"; //$NON-NLS-1$

  /**
   * Number of rows to sample
   */
  private String m_numRowsToSample = "0";

  /**
   * Random seed for reserviour sampling
   */
  private String m_randomSeed = "1";

  /**
   * don't sample, stream instead
   */
  private boolean m_streamData;

  /**
   * Class attribute/column?
   */
  private boolean m_setClass;

  /**
   * The Class attribute name
   */
  private String m_classAttribute;

  /**
   * True if the output structure has been determined successfully in advance of the input data being seen in its
   * entirety
   */
  private boolean m_structureDetermined;

  /**
   * Set whether to store the XML flow description as part of the step meta data. In this case the source file path is
   * ignored (and cleared for that matter)
   *
   * @param s true if the flow should be stored in the step meta data
   */
  public void setStoreFlowInStepMetaData( boolean s ) {
    m_storeFlowInStepMetaData = s;
  }

  /**
   * Get whether to store the XML flow description as part of the step meta data. In this case the source file path is
   * ignored (and cleared for that matter)
   *
   * @return true if the flow should be stored in the step meta data
   */
  public boolean getStoreFlowInStepMetaData() {
    return m_storeFlowInStepMetaData;
  }

  /**
   * Set the relation name to use for the sampled data.
   *
   * @param relationName the relation name to use
   */
  public void setSampleRelationName( String relationName ) {
    m_sampleRelationName = relationName;
  }

  /**
   * Get the relation name to use for the sampled data.
   *
   * @return the relation name to use
   */
  public String getSampleRelationName() {
    return m_sampleRelationName;
  }

  /**
   * Get the number of rows to randomly sample.
   *
   * @return the number of rows to sample
   */
  public String getSampleSize() {
    return m_numRowsToSample;
  }

  /**
   * Set the number of rows to randomly sample.
   *
   * @param size the number of rows to sample
   */
  public void setSampleSize( String size ) {
    m_numRowsToSample = size;
  }

  /**
   * Get the random seed to use for sampling.
   *
   * @return the random seed
   */
  public String getRandomSeed() {
    return m_randomSeed;
  }

  /**
   * Set the random seed to use for sampling rows.
   *
   * @param seed the seed to use
   */
  public void setRandomSeed( String seed ) {
    m_randomSeed = seed;
  }

  /**
   * Get whether incoming kettle rows are to be passed through to any downstream kettle steps (rather than output of
   * knowledge flow being passed on)
   *
   * @return true if rows are to be passed on to downstream kettle steps
   */
  public boolean getPassRowsThrough() {
    return m_passRowsThrough;
  }

  /**
   * Set whether incoming kettle rows are to be passed through to any downstream kettle steps (rather than output of the
   * knowledge flow being passed on).
   *
   * @param p true if rows are to be passed on to downstream kettle steps
   */
  public void setPassRowsThrough( boolean p ) {
    m_passRowsThrough = p;
  }

  /**
   * Set the file name of the serialized Weka flow to load/import from.
   *
   * @param fFile the file name
   */
  public void setSerializedFlowFileName( String fFile ) {
    m_flowFileName = fFile;
  }

  /**
   * Get the file name of the serialized Weka flow to load/import from.
   *
   * @return the file name of the serialized Weka flow
   */
  public String getSerializedFlowFileName() {
    return m_flowFileName;
  }

  /**
   * Set the actual knowledgeflow flows to run.
   *
   * @param flow the flows to run
   */
  public void setFlow( String flow ) {
    m_flow = flow;
  }

  /**
   * Get the knowledgeflow flow to be run.
   *
   * @return the flow to be run
   */
  public String getFlow() {
    return m_flow;
  }

  /**
   * Set the name of the step to inject data into.
   *
   * @param isn the name of the step to inject data into
   */
  public void setInjectStepName( String isn ) {
    m_injectStepName = isn;
  }

  /**
   * Get the name of the step to inject data into.
   *
   * @return the name of the step to inject data into
   */
  public String getInjectStepName() {
    return m_injectStepName;
  }

  /**
   * Set the name of the event to use for injecting.
   *
   * @param ien the name of the event to use for injecting
   */
  public void setInjectConnectionName( String ien ) {
    m_injectEventName = ien;
  }

  /**
   * Get the name of the event to use for injecting.
   *
   * @return the name of the event to use for injecting
   */
  public String getInjectConnectionName() {
    return m_injectEventName;
  }

  /**
   * Set the name of the step to listen to for output.
   *
   * @param osn the name of the step to listen to for output
   */
  public void setOutputStepName( String osn ) {
    m_outputStepName = osn;
  }

  /**
   * Get the name of the step to listen to for output.
   *
   * @return the name of the step to listen to for output
   */
  public String getOutputStepName() {
    return m_outputStepName;
  }

  /**
   * Set the name of the connection to use for output from the flow
   *
   * @param oen the name of the connection to use for output from the flow
   */
  public void setOutputConnectionName( String oen ) {
    m_outputConnectionName = oen;
  }

  /**
   * Get the name of the connection to use for output from the flow
   *
   * @return the name of the connection to use for output from the flow
   */
  public String getOutputConnectionName() {
    return m_outputConnectionName;
  }

  /**
   * Set whether to set a class index in the sampled data.
   *
   * @param sc true if a class index is to be set in the data
   */
  public void setSetClass( boolean sc ) {
    m_setClass = sc;
  }

  /**
   * Get whether a class index is to be set in the sampled data.
   *
   * @return true if a class index is to be set in the sampled data
   */
  public boolean getSetClass() {
    return m_setClass;
  }

  /**
   * Set the name of the attribute to be set as the class attribute.
   *
   * @param ca the name of the class attribute
   */
  public void setClassAttributeName( String ca ) {
    m_classAttribute = ca;
  }

  /**
   * Get the name of the attribute to be set as the class attribute.
   *
   * @return the name of the class attribute
   */
  public String getClassAttributeName() {
    return m_classAttribute;
  }

  /**
   * Set whether data should be streamed to the knowledge flow when injecting rather than batch injected.
   *
   * @param sd true if data should be streamed
   */
  public void setStreamData( boolean sd ) {
    m_streamData = sd;
  }

  /**
   * Get whether data is to be streamed to the knowledge flow when injecting rather than batch injected.
   *
   * @return true if data is to be streamed
   */
  public boolean getStreamData() {
    return m_streamData;
  }

  /**
   * Set the array of meta data for the inject step
   *
   * @param am an array of ArffMeta
   */
  public void setInjectFields( ArffMeta[] am ) {
    m_injectFields = am;
  }

  /**
   * Get the meta data for the inject step
   *
   * @return an array of ArffMeta
   */
  public ArffMeta[] getInjectFields() {
    return m_injectFields;
  }

  /**
   * Return the XML describing this (configured) step
   *
   * @return a <code>String</code> containing the XML
   */
  @Override public String getXml() {
    StringBuilder retval = new StringBuilder( 100 );

    retval.append( "<" + XML_TAG + ">" ); //$NON-NLS-1$ //$NON-NLS-2$

    retval.append( XmlHandler.addTagValue( "store_flow_in_meta", //$NON-NLS-1$
        m_storeFlowInStepMetaData ) );

    retval.append( XmlHandler.addTagValue( "inject_step", m_injectStepName ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "inject_event", m_injectEventName ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "output_step", m_outputStepName ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "output_event", m_outputConnectionName ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "pass_rows_through", m_passRowsThrough ) ); //$NON-NLS-1$

    // can we save the flow as XML?
    if ( !org.apache.hop.core.util.Utils.isEmpty( m_flow ) && org.apache.hop.core.util.Utils.isEmpty( m_flowFileName ) ) {
      retval.append( XmlHandler.addTagValue( "kf_flow", m_flow ) ); //$NON-NLS-1$
    } else {
      retval.append( XmlHandler.addTagValue( "flow_file_name", m_flowFileName ) ); //$NON-NLS-1$
    }

    retval.append( "    <arff>" ).append( Const.CR ); //$NON-NLS-1$
    if ( m_injectFields != null ) {
      for ( ArffMeta m_injectField : m_injectFields ) {
        if ( m_injectField != null ) {
          retval.append( "        " ).append( m_injectField.getXML() ) //$NON-NLS-1$
              .append( Const.CR );
        }
      }
    }
    retval.append( "    </arff>" + Const.CR ); //$NON-NLS-1$

    retval.append( XmlHandler.addTagValue( "stream_data", m_streamData ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "sample_relation_name", //$NON-NLS-1$
        m_sampleRelationName ) );
    retval.append( XmlHandler.addTagValue( "reservoir_size", m_numRowsToSample ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "random_seed", m_randomSeed ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "set_class", m_setClass ) ); //$NON-NLS-1$
    retval.append( XmlHandler.addTagValue( "class_attribute", m_classAttribute ) ); //$NON-NLS-1$

    retval.append( "</" + XML_TAG + ">" ); //$NON-NLS-1$ //$NON-NLS-2$

    return retval.toString();
  }

  /**
   * Loads the meta data for this (configured) step from XML.
   *
   * @param stepnode  the step to load
   * @throws HopXmlException if an error occurs
   */
  @Override public void loadXml( Node stepnode, IHopMetadataProvider provider )
      throws HopXmlException {
    int nrModels = XmlHandler.countNodes( stepnode, XML_TAG );

    if ( nrModels > 0 ) {
      Node wekanode = XmlHandler.getSubNodeByNr( stepnode, XML_TAG, 0 );

      // try and get the XML-based knowledge flow
      boolean success = false;

      try {
        m_flow = XmlHandler.getTagValue( wekanode, "kf_flow" ); //$NON-NLS-1$

        if ( !org.apache.hop.core.util.Utils.isEmpty( m_flow ) ) {
          success = true;
        }
      } catch ( Exception ex ) {
        success = false;
      }

      if ( !success ) {
        m_flowFileName = XmlHandler.getTagValue( wekanode, "flow_file_name" ); //$NON-NLS-1$
      }

      String store = XmlHandler.getTagValue( wekanode, "store_flow_in_meta" ); //$NON-NLS-1$
      if ( !org.apache.hop.core.util.Utils.isEmpty( store ) ) {
        m_storeFlowInStepMetaData = store.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      m_injectStepName = XmlHandler.getTagValue( wekanode, "inject_step" ); //$NON-NLS-1$
      m_injectEventName = XmlHandler.getTagValue( wekanode, "inject_event" ); //$NON-NLS-1$
      m_outputStepName = XmlHandler.getTagValue( wekanode, "output_step" ); //$NON-NLS-1$
      m_outputConnectionName = XmlHandler.getTagValue( wekanode, "output_event" ); //$NON-NLS-1$

      String temp = XmlHandler.getTagValue( wekanode, "pass_rows_through" ); //$NON-NLS-1$
      m_passRowsThrough = !temp.equalsIgnoreCase( "N" );

      Node fields = XmlHandler.getSubNode( wekanode, "arff" ); //$NON-NLS-1$
      int nrfields = XmlHandler.countNodes( fields, ArffMeta.XML_TAG );

      m_injectFields = new ArffMeta[nrfields];

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XmlHandler.getSubNodeByNr( fields, ArffMeta.XML_TAG, i );
        m_injectFields[i] = new ArffMeta( fnode );
      }

      temp = XmlHandler.getTagValue( wekanode, "stream_data" ); //$NON-NLS-1$
      if ( temp.equalsIgnoreCase( "N" ) ) { //$NON-NLS-1$
        m_streamData = false;
      } else {
        m_streamData = true;
      }

      m_sampleRelationName = XmlHandler.getTagValue( wekanode, "sample_relation_name" ); //$NON-NLS-1$
      m_randomSeed = XmlHandler.getTagValue( wekanode, "random_seed" ); //$NON-NLS-1$
      m_numRowsToSample = XmlHandler.getTagValue( wekanode, "reservoir_size" ); //$NON-NLS-1$
      m_classAttribute = XmlHandler.getTagValue( wekanode, "class_attribute" ); //$NON-NLS-1$

      temp = XmlHandler.getTagValue( wekanode, "set_class" ); //$NON-NLS-1$
      if ( temp.equalsIgnoreCase( "N" ) ) { //$NON-NLS-1$
        m_setClass = false;
      } else {
        m_setClass = true;
      }
    }
  }

  /**
   * Set up the outgoing row meta data from the supplied Instances object.
   *
   * @param insts the Instances to use for setting up the outgoing row meta data
   * @param row   holds the final outgoing row meta data
   */
  protected void setUpMetaData( Instances insts, IRowMeta row ) throws HopPluginException {
    row.clear();
    for ( int i = 0; i < insts.numAttributes(); i++ ) {
      Attribute temp = insts.attribute( i );
      String attName = temp.name();
      IValueMeta newVM = null;
      switch ( temp.type() ) {
        case Attribute.NUMERIC:
          newVM = ValueMetaFactory.createValueMeta( attName, IValueMeta.TYPE_NUMBER );
          break;
        case Attribute.NOMINAL:
        case Attribute.STRING:
          newVM = ValueMetaFactory.createValueMeta( attName, IValueMeta.TYPE_STRING );
          break;
        case Attribute.DATE:
          newVM = ValueMetaFactory.createValueMeta( attName, IValueMeta.TYPE_DATE );
          break;
        case Attribute.RELATIONAL:
          logBasic( BaseMessages.getString( PKG, "KnowledgeFlowMeta.Warning.IgnoringRelationalAttributes" ) );
      }

      if ( newVM != null ) {
        row.addValueMeta( newVM );
      }
    }
  }

  /**
   * Gets the fields
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output row metadata of the step
   * @param origin       the name of the step to use as input for the origin field in the values
   * @param info         fields used as extra lookup information
   * @param nextStep     the next step that is targeted
   * @param space        the variable space to use to replace variables
   * @param provider    the MetaStore to use to load additional external data or metadata impacting the output fields
   * @throws HopTransformException if a problem occurs
   */
  public void getFields( IRowMeta inputRowMeta, String origin, IRowMeta[] info, TransformMeta nextStep,
      IVariables space, IHopMetadataProvider provider) throws HopTransformException {
    Flow flow = null;

    try {
      if ( !org.apache.hop.core.util.Utils.isEmpty( getFlow() ) ) {
        flow = PMIFlowExecutorData.getFlowFromJSON( getFlow() );
      } else if ( !org.apache.hop.core.util.Utils.isEmpty( getSerializedFlowFileName() ) ) {
        flow =
            PMIFlowExecutorData.getFlowFromFileVFS( getSerializedFlowFileName(), space, Environment.getSystemWide() );
      }
    } catch ( Exception ex ) {
      throw new HopTransformException( ex );
    }

    // if we pass rows through, or there is no flow, then there is nothing to be done
    if ( getPassRowsThrough() || flow == null ) {
      return;
    }

    // validate output setup
    try {
      StepManagerImpl
          outputStep =
          PMIFlowExecutorData
              .validateOutputStep( flow, getOutputStepName(), getOutputConnectionName(), null, space, getLog() );

      String outputConnectionName = space.environmentSubstitute( getOutputConnectionName() );
      Instances outputStructure = null;
      try {
        outputStructure = outputStep.getManagedStep().outputStructureForConnectionType( outputConnectionName );
      } catch ( WekaException ex ) {
        throw new HopTransformException( ex );
      }

      if ( outputStructure != null ) {
        try {
          setUpMetaData( outputStructure, inputRowMeta );
          m_structureDetermined = true;
        } catch ( HopPluginException ex ) {
          throw new HopTransformException( ex );
        }
      } else {
        try {
          // if we are listening to a text connection then it's easy to construct output row metadata
          if ( outputConnectionName.equalsIgnoreCase( "text" ) ) {
            inputRowMeta.clear();
            inputRowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Title", IValueMeta.TYPE_STRING ) );
            inputRowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Text", IValueMeta.TYPE_STRING ) );
            m_structureDetermined = true;
          } else if ( outputConnectionName.equalsIgnoreCase( StepManager.CON_BATCH_CLASSIFIER ) || outputConnectionName
              .equalsIgnoreCase( StepManager.CON_INCREMENTAL_CLASSIFIER ) || outputConnectionName
              .equalsIgnoreCase( StepManager.CON_BATCH_CLUSTERER ) || outputConnectionName
              .equalsIgnoreCase( StepManager.CON_BATCH_ASSOCIATOR ) ) {

            // Outputs serialized associator *if* associator does not produce AssociationRules objects
            // (which will be determined above if there is a non-null set of instances from
            // outputStructureForConnectionType. TODO might want to revisit this
            String schemeType = "Classifier";
            if ( outputConnectionName.equalsIgnoreCase( StepManager.CON_BATCH_CLUSTERER ) ) {
              schemeType = "Clusterer";
            } else if ( outputConnectionName.equalsIgnoreCase( StepManager.CON_BATCH_ASSOCIATOR ) ) {
              schemeType = "Asssociator";
            }

            // classifier connections - output name, options and serialized classifier
            inputRowMeta.clear();
            inputRowMeta.addValueMeta(
                ValueMetaFactory.createValueMeta( schemeType + "_name", IValueMeta.TYPE_STRING ) );
            inputRowMeta.addValueMeta(
                ValueMetaFactory.createValueMeta( schemeType + "_options", IValueMeta.TYPE_STRING ) );
            inputRowMeta.addValueMeta( ValueMetaFactory.createValueMeta( schemeType, IValueMeta.TYPE_BINARY ) );
            m_structureDetermined = true;
          } else {
            m_structureDetermined = false;
          }
        } catch ( HopPluginException ex ) {
          throw new HopTransformException( ex );
        }
      }
    } catch ( HopException ex ) {
      throw new HopTransformException( ex );
    }
  }

  /**
   * Returns whether we have been able to successfully determine the structure of the output (in advance of seeing all
   * the input rows).
   *
   * @return true if the output structure has been determined.
   */
  public boolean isOutputStructureDetermined() {
    return m_structureDetermined;
  }

  /**
   * Check for equality
   *
   * @param obj an <code>Object</code> to compare with
   * @return true if equal to the supplied object
   */
  @Override public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      PMIFlowExecutorMeta m = (PMIFlowExecutorMeta) obj;
      return ( getXml() == m.getXml() );
    }
    return false;
  }

  /**
   * Clone this step's meta data
   *
   * @return the cloned meta data
   */
  public Object clone() {
    return (PMIFlowExecutorMeta) super.clone();
  }

  /*
 * (non-Javadoc)
 *
 * @see org.pentaho.di.trans.step.StepMetaInterface#setDefault()
 */
  @Override public void setDefault() {
    m_flowFileName = null;
    m_injectStepName = null;
    m_injectEventName = null;
    m_outputStepName = null;
    m_outputConnectionName = null;
    m_passRowsThrough = false;
    m_flow = null;
    m_numRowsToSample = "0"; //$NON-NLS-1$
    m_randomSeed = "1"; //$NON-NLS-1$
    m_streamData = false;
    m_setClass = false;
    m_classAttribute = null;
  }

  @Override public String getDialogClassName() {
    return PMIFlowExecutorDialog.class.getCanonicalName();
  }


  @Override
  public ITransform createTransform( TransformMeta transformMeta, PMIFlowExecutorData stepDataInterface, int i, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    return new PMIFlowExecutor( transformMeta, this, stepDataInterface, i, pipelineMeta, pipeline );
  }

  @Override public PMIFlowExecutorData getTransformData() {
    return new PMIFlowExecutorData();
  }
}
