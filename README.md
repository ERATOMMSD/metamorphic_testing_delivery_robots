# Metamorphic Testing of an Autonomous Delivery Robots Scheduler

Delivery systems operated by autonomous robots use schedulers to allocate robots to the different orders. Such schedulers are often optimisation-based algorithms that aim to maximise the number of delivered goods. The oracle problem affects the testing of these schedulers, as it is not always possible to assess whether the schedule produced for a given scenario is the optimal one. In this work, we propose a framework, based on a novel use of metamorphic testing, to assess the optimality of the scheduling algorithm developed by Panasonic for the management of a fleet of autonomous delivery robots in the Fujisawa Sustainable Smart Town, Japan. In the framework, a *metamorphic relation* (MR) transforms a *source test case* in a *follow-up test case* in a predefined way, and compares the results of the execution of the two tests in a simulated environment: if the comparison violates the expected relation, we can claim that one of the two schedules produced by the scheduler is suboptimal. We propose 19 MRs that target different aspects of the delivery system. Experiments over more than 900,000 test cases show that the different MRs have different abilities in exposing sub-optimal behaviour and, overall, the different relations do not subsume each other. Moreover, they also show that MR violations can provide useful insights into the scheduler's behaviour to Panasonic's engineers.

## Code
A private simulator is needed to run this code. However, results from our experiments are shared, and can be analysed using the code in [analyse_results.ipynb](metamorphic%2Fanalyse_results.ipynb)


## People
* Thomas Laurent https://laurenttho3.github.io/
* Paolo Arcaini http://group-mmm.org/~arcaini/
* Xiao-Yi Zhang https://group-mmm.org/~xiaoyi/
* Fuyuki Ishikawa http://research.nii.ac.jp/~f-ishikawa/en/
