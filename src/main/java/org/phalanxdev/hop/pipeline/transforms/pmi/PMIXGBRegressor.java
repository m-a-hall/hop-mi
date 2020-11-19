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

/**
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
@Transform( id = "PMIXGBRegressor", image = "WEKAS.svg", name = "PMI Extreme Boosting Regressor", description = "Train and evaluate an extreme gradient boosting (xgboost) regressor", categoryDescription = "PMI" )
public class PMIXGBRegressor extends BaseSupervisedPMIMeta {
  public PMIXGBRegressor() {
    setSchemeName( "Extreme gradient boosting regressor" );
  }
}
