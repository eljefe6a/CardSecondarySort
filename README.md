CardSecondarySort
=================

Hadoop Secondary Sort example using playing cards.  This code example uses the new API.

Given Input
-----------

hearts 9

spades 10

hearts 5

diamonds 5

hearts 8

hearts 3

clubs 9

hearts 1

clubs 5

clubs 3

diamonds 7

diamonds 8

spades 9

spades 1



Desired Output
--------------

We want a reducer call with all of the values for a single suit in sorted order.  The reducer calls should look like:


####R0

HEARTS,[1,3,5,8,9]


####R1
--

DIAMONDS,[5,7,8]


####R2
--

SPADES,[1,9,10]


####R3
--

CLUBS,[3,5,9]



To accomplish this goal, you need to use a Secondary Sort.  This code gives an example of that with comment in the code showing what is happening.
