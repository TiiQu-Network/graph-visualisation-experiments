# RDF File Generation
### The repository consists of notebook which looks into generating RDF files for the graph modeling task.

## Features
### At the moment, we are considering the following classes and properties
- Classes - Question, Answer, Topic and Sub-topic
- Properties - hasAnswer, hasTopic, hasSubtopic

## Getting started
- To recreate the work, clone this repository.
- If working on a local machine, open CMD or terminal in working directory.
    - Run the following command to download dependecies.
        ```
        pip install -r requirements.txt
        ```
    - Load the notebook in desired IDE and run the cells to recreate the output.
- If working on colab, load the notebook file on colab environment and install the dependencies following the sample command below, when prompted.
    ```
    !pip install <name-of-library>
    ```


## Outputs
### The outputs of the notebook are saved in two files <i>output.ttl</i> and <i>output.nt</i> giving the corresponding turtle and n-triples file.

## References
- [RDFLib docs](https://rdflib.readthedocs.io/en/stable/)
- [Semantic web tutorial](https://www.youtube.com/watch?v=e5RPhWIBcY4&list=PLea0WJq13cnDDe8V7eVLReIaOnFztOEAq)
