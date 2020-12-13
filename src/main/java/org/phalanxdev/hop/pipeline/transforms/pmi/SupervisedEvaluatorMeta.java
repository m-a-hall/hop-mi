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

import org.phalanxdev.hop.ui.pipeline.pmi.SupervisedEvaluatorDialog;
import org.phalanxdev.hop.MetaHelper;
import org.phalanxdev.hop.SimpleStepOption;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;
import weka.core.Attribute;

import java.util.Arrays;

/**
 * Simple step that computes supervised evaluation metrics from incoming ground truth class values and predicted
 * class values (as produced as output from a machine learning scheme). Can handle both numeric and nominal classes.
 * When the class is nominal, it is assumed that the predicted values are in the form of a probability distribution for
 * each row. If the class column is called "class", and it is numeric, then the step will look for a incoming field
 * called "predicted_class". If the class is nominal, then the step will determine which values it can take on by looking
 * for fields called "predicted_class_&ltlabel1&gt", "predicted_class_&ltlabel2&gt"..., where "label1", "label2" etc. are
 * the legal values that the class can assume, and the values of these fields are the predicted probabilites associated with
 * each label for the given instance (row).
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
@Transform( id = "SupervisedEvaluator", image = "WEKAS.svg", name = "Supervised Evaluator", description = "Compute supervised evaluation metrics for incoming row data that contains predictions from a learning scheme", categoryDescription = "PMI" )
public class SupervisedEvaluatorMeta extends BaseTransformMeta implements
    ITransformMeta<SupervisedEvaluator, SupervisedEvaluatorData> {

  protected String m_className = "";

  protected boolean m_outputIRStats;

  protected boolean m_outputAUC;

  @SimpleStepOption public void setClassName( String name ) {
    m_className = name;
  }

  public String getClassName() {
    return m_className;
  }

  @SimpleStepOption public void setOutputIRStats( boolean output ) {
    m_outputIRStats = output;
  }

  public boolean getOutputIRStats() {
    return m_outputIRStats;
  }

  @SimpleStepOption public void setOutputAUC( boolean output ) {
    m_outputAUC = output;
  }

  public boolean getOutputAUC() {
    return m_outputAUC;
  }

  @Override public void setDefault() {
    m_outputIRStats = false;
    m_outputAUC = false;
  }

  @Override public String getXml() {
    try {
      return MetaHelper.getXMLForTarget( this ).toString();
    } catch ( Exception ex ) {
      ex.printStackTrace();
      return "";
    }
  }

  @Override public void loadXml( Node transformNode, IHopMetadataProvider metaStore ) {
    try {
      MetaHelper.loadXMLForTarget( transformNode, this );
    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  @Override
  public SupervisedEvaluatorData getTransformData( ) {
    return new SupervisedEvaluatorData( );
  }

  @Override public ITransform createTransform( TransformMeta transformMeta, SupervisedEvaluatorData data, int copyNr,
      PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new SupervisedEvaluator( transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  public String getDialogClassName() {
    return SupervisedEvaluatorDialog.class.getCanonicalName();
  }

  protected static Attribute createClassAttribute( String className, String nominalVals ) {
    Attribute classA = null;
    if ( !org.apache.hop.core.util.Utils.isEmpty( nominalVals ) ) {
      String[] labels = nominalVals.split( "," );
      for ( int i = 0; i < labels.length; i++ ) {
        labels[i] = labels[i].trim();
      }
      classA = new Attribute( className, Arrays.asList( labels ) );
    } else {
      // assume numeric class
      classA = new Attribute( className );
    }
    return classA;
  }

  @Override public void getFields( IRowMeta rowMeta, String stepName, IRowMeta[] info, TransformMeta nextTransform,
      IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    if ( rowMeta != null && rowMeta.size() > 0 && !org.apache.hop.core.util.Utils.isEmpty( getClassName() ) ) {
      // String nominalVals = space.resolve( getNominalLabelList() );
      String className = space.resolve( getClassName() );
      try {
        // GeneralSupervisedEvaluatorUtil eval = new GeneralSupervisedEvaluatorUtil( rowMeta, className, nominalVals );
        // trans.getPrevStepFiel;
        GeneralSupervisedEvaluatorUtil eval = new GeneralSupervisedEvaluatorUtil( rowMeta, className );
        eval.getOutputFields( rowMeta, getOutputIRStats(), getOutputAUC() );
      } catch ( HopException e ) {
        throw new HopTransformException( e );
      }
    } else if ( rowMeta != null ) {
      rowMeta.clear();
    }
  }
}
