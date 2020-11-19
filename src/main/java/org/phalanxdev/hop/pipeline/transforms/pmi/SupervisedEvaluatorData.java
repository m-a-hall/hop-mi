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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

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
public class SupervisedEvaluatorData extends BaseTransformData implements ITransformData {
  protected GeneralSupervisedEvaluatorUtil m_evaluatorUtil;
  protected IRowMeta m_outputRowMeta;
}
