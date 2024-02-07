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

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DistributionTest {
    private static final int SAMPLES = 30;
    private static final Well19937c rnd = new Well19937c(42);
    private static final Logger LOG = LoggerFactory.getLogger(DistributionTest.class);

    public static void testZipfDistribution() {
        int NUM = 10;
        ZipfDistribution zDistribution = new ZipfDistribution(rnd, NUM, 1.1);

        double acc = 0;
        for (int i = 1; i <= NUM; i++) {
            LOG.info("Probability of " + i + " : " + zDistribution.probability(i));
            acc += zDistribution.probability(i);
        }
        LOG.info("Comulative prop:" + acc);

        for (int i = 0; i < SAMPLES; i++) {
            int sample = zDistribution.sample();
            LOG.info("sample:" + sample + "");
        }
    }

    public static void testUniformDistribution() {
        int lw = 1, up = 10;
        UniformIntegerDistribution uDistribution = new UniformIntegerDistribution(rnd, lw, up);

        double acc = 0;
        for (int i = lw; i <= up; i++) {
            LOG.info("Probability of " + i + " : " + uDistribution.probability(i));
            acc += uDistribution.probability(i);
        }
        LOG.info("Comulative prop:" + acc);

        for (int i = 0; i < SAMPLES; i++) {
            int sample = uDistribution.sample();
            LOG.info("sample:" + sample + "");
        }
    }
}
