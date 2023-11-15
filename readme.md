# Process large csv file with Multiprocessing module

#### This program splits a large data file to chunks and does a parallel computations for each chunk.
#### The file contains some data about trips by NY taxy companies. We need to calculate a total revenue of each company



```mermaid
flowchart TB;
    S((Start))
    style S fill:blue
    F{Does file exist?}
    style F fill:orange
    E(IOError)
    style E fill:red
    D(Divide File to Chunks)
    style D fill:brown
    CR(Create Processes)
    style CR fill:green
    P1{{Process Chunk 1}}
    style P1 fill:violet
    P2{{Process Chunk 2}}
    style P2 fill:violet
    P3{{Process Chunk 3}}
    style P3 fill:violet
    P4{{Process Chunk 4}}
    style P4 fill:violet
    P5{{Process Chunk ...}}
    style P5 fill:violet
    P6{{Process Chunk N}}
    style P6 fill:violet
    Ex(Extract Results)
    style Ex fill:gray
    END((End))
    style END fill:blue
    S ----> F
    F -->|yes| D
    F ---->|no| E
    D -->CR
    CR ----> P1
    CR ----> P2
    CR ----> P3
    CR ----> P4
    CR ----> P5
    CR ----> P6
    P1 ----> Ex
    P2 ----> Ex
    P3 ----> Ex
    P4 ----> Ex
    P5 ----> Ex
    P6 ----> Ex
    E ----> END
    Ex ----> END

```