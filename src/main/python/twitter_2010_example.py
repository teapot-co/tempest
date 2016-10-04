#!/usr/bin/env python
# Connects to a local tempest server on post 10001, assuming that the underlying dataset is twitter_2010
# Returns the top 10 influencers for a given uername
# Usage: twitter_2010_influencers.py <twitter_user_name>
# Can also be safely loaded from the python shell for interactive use.


import tempest_db
import sys

default_num_steps = 100000
default_reset_probability = 0.3

def get_influencers(graph_name, username, client, num_results = 10):
    user = client.nodes(graph_name, "username = '" + username + "'")
    if len(user) != 1:
        return "Unknown User"
    ppr_map = client.ppr(graph_name, user, max_results=num_results+1, num_steps = default_num_steps, reset_probability=default_reset_probability)
    influencer_ids = filter(lambda id: id != user[0],
        sorted(ppr_map, key = lambda id: -ppr_map[id]))
    id_to_username = client.multi_node_attribute(graph_name, influencer_ids, "username")
    return [id_to_username[i] for i in influencer_ids]

def get_recommendations(graph_name, username, client, num_results = 10):
    user = client.nodes("username = '" + username + "'")
    if len(user) != 1:
        return "Unknown User"
    user_id = user[0]
    out_degree = client.out_degree(user_id)
    if out_degree > 5000:
        return "Overly active user, no recommendations possible"
    neighbors = set(client.out_neighbors(user_id))
    neighbors.add(user_id)
    num_results_to_fetch = num_results + len(neighbors)
    if len(neighbors) < 5:
        return "Inactive user, no recommendations possible"
    ppr_map = client.ppr(graph_name, user, max_results=num_results_to_fetch, num_steps = default_num_steps, reset_probability=default_reset_probability)
    influencer_ids = sorted(ppr_map, key = lambda id: -ppr_map[id])
    recommended_ids = filter(lambda id: id not in neighbors, influencer_ids)[0:num_results]
    id_to_username = client.multi_node_attribute(graph_name, recommended_ids, "username")
    return [id_to_username[i] for i in recommended_ids]

if __name__ == "__main__":
    if len(sys.argv) == 2:
        print "Note: this is from a snapshot of Twitter from 2010. Consequently, the results will be stale. Please don't use this script in a production setting."
        client = tempest_db.TempestClient()
        print "TOP INFLUENCERS: ", get_influencers("twitter", sys.argv[1], client)
        print "TOP RECOMMENDATIONS: ", get_recommendations("twitter", sys.argv[1], client)
        client.close()
    else:
        print "Usage: " + sys.argv[0] + " <twitter_user_name>"

