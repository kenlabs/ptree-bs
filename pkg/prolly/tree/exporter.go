package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/awalterschulze/gographviz"
	"github.com/ipfs/go-cid"
	"io/ioutil"
	"os/exec"
	"ptree-bs/pkg/prolly/tree/schema"
	"runtime"
)

func ExportTreeToDot(ctx context.Context, tree *StaticTree, hideLeaf bool, name string) (string, error) {
	type graphNode struct {
		nd   schema.ProllyNode
		c    cid.Cid
		name string
	}

	graphAst, err := gographviz.ParseString(`digraph G {
	}`)
	if err != nil {
		return "", err
	}
	graph := gographviz.NewGraph()
	if err := gographviz.Analyse(graphAst, graph); err != nil {
		panic(err)
	}

	// the package not support set global node attrs, we must apply it in each node
	nodeGlobalAttrs := make(map[string]string)
	nodeGlobalAttrs["shape"] = "box"
	leafNodesAttrs := make(map[string]string)
	leafNodesAttrs["shape"] = "plaintext"

	graph.AddAttr("G", "rankdir", "LR")

	// todo  may be tree struct should save the root cid
	c, err := tree.Ns.WriteNode(ctx, tree.Root, nil)
	if err != nil {
		return "", err
	}
	graph.AddNode("G", "Root_"+c.String(), nodeGlobalAttrs)

	queue := make([]graphNode, 0)
	queue = append(queue, graphNode{tree.Root, c, "Root_" + c.String()})
	for len(queue) != 0 {
		gnd := queue[0]
		queue = queue[1:]
		if !gnd.nd.IsLeaf() {
			for i := 0; i < gnd.nd.ItemCount(); i++ {
				nd, err := tree.Ns.ReadNode(ctx, gnd.nd.GetAddress(i))
				if err != nil {
					return "", err
				}
				childCid := gnd.nd.GetAddress(i)
				newNode := graphNode{
					nd:   nd,
					c:    childCid,
					name: childCid.String()[:4] + `_` + childCid.String()[28:],
				}
				err = graph.AddNode("G", newNode.name, nodeGlobalAttrs)
				if err != nil {
					return "", err
				}
				// not set in the global map, it will pollute others attrs
				graph.Nodes.Lookup[newNode.name].Attrs["tooltip"] = childCid.String()
				err = graph.AddEdge(gnd.name, newNode.name, true, map[string]string{"label": "KEY_" + string(gnd.nd.GetKey(i))})
				if err != nil {
					return "", err
				}
				queue = append(queue, newNode)
			}
		} else {
			if !hideLeaf {
				for i := 0; i < gnd.nd.ItemCount(); i++ {
					vNodeName := string(gnd.nd.GetValue(i))
					err = graph.AddNode("G", vNodeName, leafNodesAttrs)
					if err != nil {
						return "", err
					}
					err = graph.AddEdge(gnd.name, vNodeName, true, map[string]string{"label": "KEY_" + string(gnd.nd.GetKey(i))})
					if err != nil {
						return "", err
					}
				}
			}
		}
	}
	dotFileName := name + ".dot"
	pngFileName := name + ".svg"
	err = ioutil.WriteFile(dotFileName, []byte(graph.String()), 0666)
	if err != nil {
		return "", err
	}

	system(fmt.Sprintf("dot %s -T svg -o %s", dotFileName, pngFileName))

	return graph.String(), nil
}

func system(s string) {
	var cmd *exec.Cmd
	osType := runtime.GOOS
	if osType == "windows" {
		cmd = exec.Command(`cmd`, "/C", s)
	} else {
		cmd = exec.Command(`/bin/sh`, `-c`, s)
	}

	var out bytes.Buffer

	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		fmt.Printf("falied to generated png file from dot auto, err: %v", err)
	}
	fmt.Printf("%s", out.String())
}
