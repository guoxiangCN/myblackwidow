#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

template <size_t kNumVertex>
class Graph {
 public:
  using VertexHandle = uint64_t;

  Graph() {
    vertexs_.reserve(kNumVertex);
    for (auto& x : edges_) {
      for (auto& y : x) {
        y = 0;
      }
    }
  }

  VertexHandle AddVertex(const std::string& vertex) {
    auto it = std::find(vertexs_.begin(), vertexs_.end(), vertex);
    if (it != vertexs_.end()) {
      return it - vertexs_.begin();
    }
    vertexs_.push_back(vertex);
    return vertexs_.size() - 1;
  }

#if __cplusplus < 201400L
  typedef uint64_t (&ret_array_ref_t)[kNumVertex];
  ret_array_ref_t operator[](size_t pos) {
#else
  auto& operator[](size_t pos) {
#endif
    return edges_[pos];
  }

  void AddEdge(const VertexHandle &r, const VertexHandle & c, uint64_t weight) {
    edges_[r][c] = weight;
    edges_[c][r] = weight;
  }

  void Display() {
    for (auto& x : edges_) {
      for (auto& y : x) {
        std::cout << y << " ";
      }
      std::cout << std::endl;
    }
  }

 private:
  std::vector<std::string> vertexs_;
  uint64_t edges_[kNumVertex][kNumVertex];
};


int main(int argc, char** argv) {
  Graph<5> graph;
  Graph::VertexHandle a = graph.AddVertex("A");
  Graph::VertexHandle b = graph.AddVertex("B");
  Graph::VertexHandle c = graph.AddVertex("C");
  Graph::VertexHandle d = graph.AddVertex("D");
  Graph::VertexHandle e = graph.AddVertex("E");

  graph.AddEdge(a, b, 1);
  graph.AddEdge(a, c, 1);
  graph.AddEdge(a, e, 1);


  graph.Display();

  return 0;
}