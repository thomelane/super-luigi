import json
import luigi
from typing import List, Union, Dict


try:
    from pyvis.network import Network
    import networkx as nx
except ImportError:
    pass


def dependency_graph(
    task,
    ignore_beyond: Union[str, List[str]] = []
):
    if not isinstance(ignore_beyond, list):
        ignore_beyond = [ignore_beyond]

    G = nx.DiGraph()
    node_dict: Dict[luigi.Task, int] = {}

    def _get_node_attrs(task):
        node_attrs = {}
        node_attrs['task_family'] = task.task_family
        node_attrs['task_hash'] = task.task_hash
        node_attrs['task_id'] = task.task_id
        node_attrs['params'] = {
            k: str(v) for k, v in task.param_kwargs.items()
            if k not in set(['raise_exception', 'force'])
        }
        return node_attrs

    def _construct_graph(G, node_dict, task, ignore_beyond):
        if task not in node_dict:
            node_dict[task] = len(node_dict)
            G.add_node(node_dict[task], **_get_node_attrs(task))
        for req_task in luigi.task.flatten(task.requires()):
            if req_task not in node_dict:
                node_dict[req_task] = len(node_dict)
                G.add_node(node_dict[req_task], **_get_node_attrs(req_task))
            G.add_edge(node_dict[req_task], node_dict[task])
            if not any([isinstance(req_task, cls) for cls in ignore_beyond]):
                _construct_graph(G, node_dict, req_task, ignore_beyond)

    _construct_graph(G, node_dict, task, ignore_beyond)
    return G


def plot_dependency_graph(
    task,
    ignore_beyond: List[str] = [],
    hierarchical: bool = False,
    height: str = '400px',
    font_size: int = 6,
    notebook: bool = True,
    output_path: str = 'tmp.html'
):

    G = dependency_graph(task, ignore_beyond=ignore_beyond)

    network = Network(
        height=height,
        width='100%',
        bgcolor='#222222',
        font_color='white',
        notebook=notebook
    )
    network.from_nx(G)

    for node in network.nodes:
        node['title'] = f'task_family: {node["task_family"]}<br>task_hash: {node["task_hash"]}<br><br>' + \
            "<br>".join([f'{k}: {v}' for k, v in node['params'].items()])
        node['label'] = node['task_family']
        if node["task_hash"] == task.task_hash:
            node['color'] = '#fa0a5a'

    options = {
      "nodes": {
        "font": {
          "size": font_size
        }
      },
      "edges": {
        "arrows": {
          "to": {
            "enabled": True,
            "scaleFactor": 0.5
          }
        },
        "smooth": False
      }
    }

    if hierarchical:
        options["layout"] = {
            "hierarchical": {
                "enabled": True,
                "direction": 'DU',
            }
        }

    network.set_options(json.dumps(options))
    return network.show(output_path)