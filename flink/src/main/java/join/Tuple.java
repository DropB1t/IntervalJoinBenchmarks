/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of IntervalJoinBenchmarks.
 *  
 *  IntervalJoinBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/DropB1t/IntervalJoinBenchmarks/blob/main/LICENSE
 *  
 *  IntervalJoinBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package join;

import java.io.Serializable;

public class Tuple implements Serializable {
	public int key;
	public int value;
	public long ts_off;

	public Tuple() {
		key = -1;
		value = 0;
		ts_off = 0L;
	}

	public Tuple(int _key, int _val, long _ts_off) {
		key = _key;
		value = _val;
		ts_off = _ts_off;
	}

	public Tuple(int _key, long _ts_off) {
		key = _key;
		value = 5;
		ts_off = _ts_off;
	}

	public Tuple(int _key, int _val) {
		key = _key;
		value = _val;
		ts_off = 0L;
	}

	@Override
	public String toString() {
		return key + " | " + value;
	}
}
