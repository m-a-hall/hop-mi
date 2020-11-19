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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

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
public class SupervisedEvaluator extends BaseTransform<SupervisedEvaluatorMeta, SupervisedEvaluatorData> implements
    ITransform<SupervisedEvaluatorMeta, SupervisedEvaluatorData> {

  private static Class<?> PKG = SupervisedEvaluator.class;

  protected SupervisedEvaluatorMeta m_meta;
  protected SupervisedEvaluatorData m_data;

  /**
   * Constructor
   *
   * @param transformMeta
   * @param data
   * @param copyNr
   * @param pipelineMeta
   * @param pipeline
   */
  public SupervisedEvaluator( TransformMeta transformMeta, SupervisedEvaluatorMeta meta, SupervisedEvaluatorData data, int copyNr, PipelineMeta pipelineMeta,
      Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
    m_meta = meta;
    m_data = data;
  }

  /**
   * Initializes the step and performs configuration checks
   *
   * @return true if all is good.
   */
  public boolean init( ) {

    if ( super.init( ) ) {
      try {
        if ( org.apache.hop.core.util.Utils.isEmpty( m_meta.getClassName() ) ) {
          throw new HopException( "No class field specified!" );
        }
      } catch ( Exception ex ) {
        logError( ex.getMessage(), ex );
        ex.printStackTrace();
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
  @Override public boolean processRow( ) throws HopException {
    Object[] inputRow = getRow();

    if ( first ) {
      first = false;
      m_data.m_outputRowMeta = getInputRowMeta().clone();
      m_data.m_evaluatorUtil =
          new GeneralSupervisedEvaluatorUtil( m_data.m_outputRowMeta, environmentSubstitute( m_meta.getClassName() ) );
      m_data.m_evaluatorUtil
          .getOutputFields( m_data.m_outputRowMeta, m_meta.getOutputIRStats(), m_meta.getOutputAUC() );
    }

    if ( isStopped() ) {
      return false;
    }

    if ( inputRow == null ) {
      // finished - compute eval statistics and output
      putRow( m_data.m_outputRowMeta, m_data.m_evaluatorUtil
          .getEvalRow( m_data.m_outputRowMeta, m_meta.getOutputIRStats(), m_meta.getOutputAUC() ) );
      setOutputDone();
      return false;
    } else {
      m_data.m_evaluatorUtil.evaluateForRow( getInputRowMeta(), inputRow, m_meta.getOutputAUC(), log );
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic( BaseMessages.getString( PKG, "BasePMIStep.Message.LineNumber", getLinesRead() ) );
    }

    return true;
  }
}
