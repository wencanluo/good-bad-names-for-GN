# good-bad-names-for-GN
Classify the Scientific names database as 'trusted' or 'not trusted'

[![Stories in Ready](https://badge.waffle.io/wencanluo/good-bad-names-for-GN.png?label=ready&title=Ready)](https://waffle.io/wencanluo/good-bad-names-for-GN)

[![Gitter][1]][2]


[1]: https://badges.gitter.im/Join%20Chat.svg
[2]: https://gitter.im/wencanluo/good-bad-names-for-GN?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge

## How to run it?
### requirements
  * MySQL
  * python
  * Java
  * Netiti
  * TaxonFinder
  * Parser
  * Data
    * GN database
    * VertNet data
    * datasource authority
    
### step by step to produce the results
#### 1. generate the feature table (name_string_refinery)
     Feature explaination
     https://docs.google.com/document/d/1mblzmi1o0dm70OSvR0qR7vrQ69KBONw_wroArRMeCPc/edit
#### 2. build the good-bad classifier
#### 3. run the classifier
#### 4. write back the predictions into the table
