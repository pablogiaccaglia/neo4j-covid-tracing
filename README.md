<p align="center">
  <i><font size="3">
  	Systems and Methods for Big and Unstructured Data - Delivery #1 - AA 2021/2022 - Prof. Marco Brambilla
  </i>
</p>
<h1 align="center">
	<strong>
	🦠Neo4j Covid Tracing Database
	</strong>
	<br>
</h1>
<p align="center">
<font size="3">
		<a href="https://neo4j.com/">Neo4j</a>		 
		•		
		<a href="report/report.pdf">Report</a>   
	</font>
</p>

Considering the scenario in which there’s the need to build a system for managing the **COVID-19 pandemic** in a specific country, 
our project focuses on the data perspective level. This is why we designed and implemented a **Neo4j** data structure to face the need of contact tracing functionality,
to monitor the viral diffusion. 

# Contents

- ⚙  [System requirements️](#system-requirements)
- 🚀 [Setup instructions](#-setup-instructions)
- 📜 [Report](report.pdf)
- 👨‍💻 [Usage](#-usage)
- 🗄️ [Database dump](https://1drv.ms/u/s!Ahq9yFCnfdZEjulz7J5lFAN65v9tvQ?e=MvCgVh)
- 📊 [Diagrams](#-diagrams)
- 📷 [Relationships Visualizations](#-relationships-visualizations)  
- 📝 [License](#-license)

# System requirements

## Required software

- [Python](https://www.python.org/) 3.8 or higher
- [Neo4J](https://neo4j.com) database
- Python modules in [requirements.txt](requirements.txt)


# 🚀 Setup instructions

## Clone the repo

    git clone https://github.com/pablogiaccaglia/neo4j-covid-tracing
    cd neo4j-covid-tracing/

## Install required packages

From the project's directory run the following commands:

    pip install -r requirements.txt
    
# 👨‍💻 Usage

# 📊 Diagrams

<h2><p align="center"><b>ER Diagram</b></></h2>

 <p align= "center">
 <kbd> 
 <img src="https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/ER_Neo4J.png" align="center" />
 </kbd>
 </>
---
	 
<h2><p align="center"><b>ER Diagram</b></></h2>

 <p align= "center">
 <kbd> 
 <img src="https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/neo4j-meta-graph.png" align="center" />
 </kbd>
 </>
---
	 
# 📷 Relationships Visualizations

WENT TO        |  TOOK
:-------------------------:|:-------------------------:
![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/WENT_TO.png)|  ![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/TOOK.png)

---

RECEIVED      |  PART OF
:-------------------------:|:-------------------------:
![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/RECEIVED.png)|  ![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/PART_OF.png)

---

MET           |  LOCATED
:-------------------------:|:-------------------------:
![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/MET.png)|  ![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/LOCATED.png)

---

LIVES WITH            |  LIVES IN
:-------------------------:|:-------------------------:
![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/LIVES_WITH.png)|  ![](https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/master/report/latex/LIVES_IN.png)
	 
# About database population scripts

	 
The creation script, which can be executed invoking the <a href="https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/1589bc335e250322837fed4cd52f6d46b6f016eb/scripts/main.py#L33">populateDatabase</a> method of class CovidGraphHandler located inside file <a href="https://github.com/pablogiaccaglia/neo4j-covid-tracing/blob/1589bc335e250322837fed4cd52f6d46b6f016eb/scripts/main.py">main.py</a>, takes approximately 
**6 hours to complete**. <br>
What it creates inside the Neo4j database are:
- **12014** nodes : 
   * 5000 **Person** nodes
   * 4883 **Place** nodes
   * 2123 **City** nodes
   * 4 **Vaccine** nodes
   * 3 **Test** nodes
   * 1 **Country** node
    
- **296682** directed (**593364** undirected) relationships: 
  * 2123 directed (4246 undirected) **PART OF** relationships
  * 1139 directed (2278 undirected) **LOCATED** relationships
  * 3752 directed (7504 undirected) **RECEIVED** relationships
  * 6537 directed (13074 undirected) **TOOK** relationships
  * 8441 directed (16882 undirected) **LIVES WITH** relationships
  * 5000 directed (10000 undirected) **LIVES IN** relationships
  * 119651 directed (239302 undirected) **MET** relationships
  * 150040 directed (300080 undirected) **WENT TO** relationships
	 
# 📝 License

This file is part of "Noe4j Covid Tracing Database".

"Neo4j Covid Tracing Database" is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

"Neo4j Covid Tracing Database" is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program (LICENSE.txt).  If not, see <http://www.gnu.org/licenses/>
