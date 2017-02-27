This repo contains some python scripts to work with python.
Script has dependensies on each other, so repo have to cloned,
you can't just grub and run selected scripts.

    $ git clone https://github.com/koder-ua/ceph-tools.git
    $ cd ceph-tools
    $ pip install -r requirement.txt

rebalance.py
------------
    
This script allows to gently change a weight of set of OSD by increasing/decreasing it with small step
and wait till rebalance complete.

To run it ones need to create yaml file, describing expected weights of OSD's.
Example:

    step: 0.1  # max weight change in one step for one OSD
    max_reweight: 1   # max OSD, updated in parallel
    min_weight_diff: 0.01  # minimal weight change to be applied
                           # e.g. if current weight is 1.222 and required weight is 1.223 weight would not be changed
    osds:   # list of OSD
        # root/host/name - path in CRUSH to select osd
      - osd: osd.0
        root: default 
        host: osd-0
        weight: 0.3  # new weight
      - osd: osd.1
        root: default
        host: osd-2
        weight: 0.3
      - osd: osd.2
        root: default
        host: osd-1
        weight: 0.3

Estimate how many data would be moved during rebalance:

    $ python rebalance.py -q -e rebalance.yaml
        Total bytes to be moved : 2.2 GiB
        Total PG to be moved  : 71

Run rebalance:
    
    $ python rebalance.py rebalance.yaml 
        
        osd(name=u'osd.0', weight=0.399979, id=0) => 0.9
        osd(name=u'osd.1', weight=0.299988, id=1) => 0.2
        osd(name=u'osd.2', weight=0.299988, id=2) => 0.11
        Total sum of all weight changes 0.8
        Total bytes to be moved : 2.0 GiB
        Total PG to be moved  : 63
        Waiting for cluster to complete rebalance done
        ceph osd crush set osd.0 0.499979 root=default host=osd-0
        Waiting for cluster to complete rebalance done
        Done 12%
        ceph osd crush set osd.1 0.2 root=default host=osd-2
        ....

You can stop tool at any time and then start again - it will continue
from where it finished.
    