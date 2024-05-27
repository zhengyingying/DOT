package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

public class PathTraversalOperation extends Traversal{

    public enum PathTraversal{
        path("path"),
        tree("tree");

        private String pathTraversal;

        PathTraversal(String pathTraversal){this.pathTraversal = pathTraversal;}

        public String getPathTraversal(){return pathTraversal;}
    }

    public static PathTraversal getRandomPathTraversal(){
        return Randomly.fromOptions(PathTraversal.values());
    }

    public static class Path extends  PathTraversalOperation{


        public Path(){ }

        @Override
        public String toString(){
            return "path()";
        }

    }

    public static class Tree extends  PathTraversalOperation{

        public Tree(){ }

        @Override
        public String toString(){
            return "tree()";
        }

    }

    public static Path createPath(){ return  new Path();}
    public static Tree createTree(){return new Tree();}

}
