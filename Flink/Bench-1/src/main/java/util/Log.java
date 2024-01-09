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

package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;

// Log class
public class Log {
    static {
        Level level;
        // restrict messages from this package
        level = Level.toLevel(System.getenv("LOG_LEVEL"), Level.DEBUG);
        Configurator.setLevel("join", level);
        // restrict messages from other components
        level = Level.toLevel(System.getenv("ROOT_LOG_LEVEL"), Level.ERROR);
        Configurator.setRootLevel(level);
    }

    // get method
    public static Logger get(Class<?> klass) {
        return LoggerFactory.getLogger(klass);
    }
}
