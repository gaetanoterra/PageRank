import re
import sys
from pyspark import SparkContext

#function that selects title and links of each node, returns => title, links
def findTitleAndLinks(input):
    title = re.findall("<title>(.*?)</title>", input)
    links = re.findall("\\[\\[(.*?)\\]\\]", input)

    return title[0], links

#function that assigns an initial value for the algorithm and selects node and links, returns => title, links, initial_page_rank
def contributeCalculus(line):
    page_rank = 0
    if len(line[1]) > 0:
        page_rank = line[2]/len(line[1])
    else:
        page_rank = 0
    return line[0], line[1], page_rank

#function that associates to each link (if any) the contribution it receives, returns => title (link), contribution
def getLinkContribution(line):
    if len(line[1]) > 0:
        for link in line[1]:
            yield link, line[2]
    else:
        yield line[0], 0
#function that computes page_rank, returns => node, page_rank
def rankingcalculus(line):
    return line[0], (alpha_value/node_number + (1-alpha_value) * line[1])

#function that returns node, links and, depending on whether it is a node to which nobody points or not, the constant or the page_rank with contributions
def rewrite(line):
    if line[1][1] == None:
        return line[0], line[1][0], alpha_value/node_number
    else:
        return line[0], line[1][0], line[1][1]

if __name__ == "__main__":
    input_file_name = sys.argv[1]
    output_file_name = sys.argv[2]
    alpha_value = float(sys.argv[3])
    iterations_number = int(sys.argv[4])
    sc = SparkContext("yarn", "PageRank")
    text = sc.textFile(input_file_name)
    #counting the nodes (pages) of the input document
    node_number = text.count()
    #call the findTitleAndLinks function on the document to select titles and links
    title_links_rdd = text.map(lambda line : findTitleAndLinks(line)).cache()
    initial_page_rank = 1/node_number
    # I reorder the data by putting node_name, initial_page_rank
    node_rank_rdd = title_links_rdd.map(lambda line : (line[0], initial_page_rank))
    for i in range(iterations_number):
        # i make a join between (node, links) and (node, page rank) obtaining -> node, (links, pagerank) -> after rewrite -> node, links, pagerank
        node_rank_rdd = title_links_rdd.join(node_rank_rdd).map(lambda line : rewrite(line))
        # Call the contributeCalculus function to calculate the contribution each node would make to its links
        contribute_ranking_rdd = node_rank_rdd.map(lambda line : contributeCalculus(line))
        # Call the getLinkContribution function to associate the contribution it receives with each link (node that have no inlinks are excluded brcause they are not links)
        link_contribution_association_rdd = contribute_ranking_rdd.flatMap(lambda line : getLinkContribution(line))
        # Group the nodes that have the same key (the same name) and add the partial page_ranks; then I calculate the ranking with rankingcalculus
        somma_rdd = link_contribution_association_rdd.reduceByKey(lambda x, y : x + y).map(lambda line : rankingcalculus(line))
        # We associate the value alpha/N to the nodes with no inlinks 
        no_inlink_nodes_rdd = title_links_rdd.map(lambda node: (node[0], alpha_value*(1/node_number))).subtractByKey(somma_rdd)
        # We put all nodes (nodes with no inlinks and with inlinks) together
        node_rank_rdd = somma_rdd.union(no_inlink_nodes_rdd)
    # Sort in descending order according to rank and output node + page_rank
    sort_rdd = node_rank_rdd.sortBy(lambda x: -x[1]).map(lambda line : (line[0], line[1]))
    for element in sort_rdd.collect():
        print("somma_rdd: " + str(element))
    #save the output to a file
    sort_rdd.saveAsTextFile(output_file_name)


