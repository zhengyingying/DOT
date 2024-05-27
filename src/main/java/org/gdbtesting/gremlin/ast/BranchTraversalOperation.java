package org.gdbtesting.gremlin.ast;

import org.gdbtesting.Randomly;

import java.util.List;

public class BranchTraversalOperation extends Traversal{

    public enum Branch{
        union("union"),
//        repeat("repeat"),
//        repeatEmit("repeatEmit")
//        choose("choose"),
//        optional("optional")
        ;

        private String branch;

        Branch(String branch){this.branch = branch;}

        public String getBranch(){return branch;}
    }

    public static Branch getRandomBranch(){
        return Randomly.fromOptions(Branch.values());
    }

    public static class Union extends  BranchTraversalOperation{

        private List<Traversal> traversalList;

        public Union(List<Traversal> traversalList){
            this.traversalList = traversalList;
        }

        @Override
        public String toString(){
            StringBuilder s = new StringBuilder("union(__.");
            for(int i = 0; i < traversalList.size(); i++){
                s.append(traversalList.get(i));
                if(i == traversalList.size() - 1){
                    break;
                }
                s.append(", ");
            }
            s.append(")");
            return s.toString();
        }

    }

    public static class Repeat extends  BranchTraversalOperation{

        private Traversal traversal;

        public Repeat(Traversal traversal){
            this.traversal = traversal;
        }

        @Override
        public String toString(){
            return Randomly.getBoolean() ? "repeat(__." + traversal.toString() + ").times(" + Randomly.getInteger(1, 5) + ").emit()"
                    : "emit().repeat(__." + traversal.toString() + ").times(" + Randomly.getInteger(1, 5) + ")";
        }

    }

    public static class RepeatEmit extends  BranchTraversalOperation{

        private Traversal traversal;
        private Traversal emitTraversal;

        public RepeatEmit(Traversal traversal, Traversal emitTraversal){
            this.traversal = traversal;
            this.emitTraversal = emitTraversal;
        }

        @Override
        public String toString(){
            return Randomly.getBoolean() ? "repeat(__." + traversal.toString() + ").times(" + Randomly.getInteger(1, 5) + ").emit("+ emitTraversal.toString() +")"
                    : "emit("+ emitTraversal.toString() +").repeat(__." + traversal.toString() + ").times(" + Randomly.getInteger(1, 5) + ")";
        }

    }

    public static Union createUnion(List<Traversal> traversals){ return new Union(traversals); }
    public static Repeat createRepeat(Traversal traversal){ return new Repeat(traversal);}
    public static RepeatEmit createRepeatEmit(Traversal a, Traversal b){return new RepeatEmit(a, b);}
}
