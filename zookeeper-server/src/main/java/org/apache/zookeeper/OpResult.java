package org.apache.zookeeper;

import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.List;

/**
 * 各类型操作返回结果
 * Encodes the result of a single part of a multiple operation commit.
 */
public abstract class OpResult {

    /**
     * 操作类型
     */
    private final int type;

    private OpResult(int type) {
        this.type = type;
    }

    /**
     * Encodes the return type as from ZooDefs.OpCode.  Can be used
     * to dispatch to the correct cast needed for getting the desired
     * additional result data.
     *
     * @return an integer identifying what kind of operation this result came from.
     * @see ZooDefs.OpCode
     */
    public int getType() {
        return type;
    }

    /**
     * A result from a create operation.  This kind of result allows the
     * path to be retrieved since the create might have been a sequential
     * create.
     */
    public static class CreateResult extends OpResult {

        private String path;
        private Stat stat;

        public CreateResult(String path) {
            this(ZooDefs.OpCode.create, path, null);
        }

        public CreateResult(String path, Stat stat) {
            this(ZooDefs.OpCode.create2, path, stat);
        }

        private CreateResult(int opcode, String path, Stat stat) {
            super(opcode);
            this.path = path;
            this.stat = stat;
        }

        public String getPath() {
            return path;
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CreateResult)) {
                return false;
            }

            CreateResult other = (CreateResult) o;

            boolean statsAreEqual = stat == null
                    && other.stat == null
                    || (stat != null
                    && other.stat != null
                    && stat.getMzxid() == other.stat.getMzxid());
            return getType() == other.getType() && path.equals(other.getPath()) && statsAreEqual;
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + path.hashCode() + (stat == null ? 0 : stat.getMzxid()));
        }

    }

    /**
     * A result from a delete operation.  No special values are available.
     */
    public static class DeleteResult extends OpResult {

        public DeleteResult() {
            super(ZooDefs.OpCode.delete);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DeleteResult)) {
                return false;
            }

            DeleteResult opResult = (DeleteResult) o;
            return getType() == opResult.getType();
        }

        @Override
        public int hashCode() {
            return getType();
        }

    }

    /**
     * A result from a setData operation.  This kind of result provides access
     * to the Stat structure from the update.
     */
    public static class SetDataResult extends OpResult {

        private Stat stat;

        public SetDataResult(Stat stat) {
            super(ZooDefs.OpCode.setData);
            this.stat = stat;
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SetDataResult)) {
                return false;
            }

            SetDataResult other = (SetDataResult) o;
            return getType() == other.getType() && stat.getMzxid() == other.stat.getMzxid();
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + stat.getMzxid());
        }

    }

    /**
     * A result from a version check operation.  No special values are available.
     */
    public static class CheckResult extends OpResult {

        public CheckResult() {
            super(ZooDefs.OpCode.check);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CheckResult)) {
                return false;
            }

            CheckResult other = (CheckResult) o;
            return getType() == other.getType();
        }

        @Override
        public int hashCode() {
            return getType();
        }

    }

    /**
     * A result from a getChildren operation. Provides a list which contains
     * the names of the children of a given node.
     */
    public static class GetChildrenResult extends OpResult {

        private List<String> children;

        public GetChildrenResult(List<String> children) {
            super(ZooDefs.OpCode.getChildren);
            this.children = children;
        }

        public List<String> getChildren() {
            return children;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GetChildrenResult)) {
                return false;
            }

            GetChildrenResult other = (GetChildrenResult) o;
            return getType() == other.getType() && children.equals(other.children);
        }

        @Override
        public int hashCode() {
            return getType() * 35 + children.hashCode();
        }

    }

    /**
     * A result from a getData operation. The data is represented as a byte array.
     */
    public static class GetDataResult extends OpResult {

        private byte[] data;
        private Stat stat;

        public GetDataResult(byte[] data, Stat stat) {
            super(ZooDefs.OpCode.getData);
            this.data = (data == null ? null : Arrays.copyOf(data, data.length));
            this.stat = stat;
        }

        public byte[] getData() {
            return data == null ? null : Arrays.copyOf(data, data.length);
        }

        public Stat getStat() {
            return stat;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GetDataResult)) {
                return false;
            }

            GetDataResult other = (GetDataResult) o;
            return getType() == other.getType() && stat.equals(other.stat) && Arrays.equals(data, other.data);
        }

        @Override
        public int hashCode() {
            return (int) (getType() * 35 + stat.getMzxid() + Arrays.hashCode(data));
        }

    }

    /**
     * An error result from any kind of operation.  The point of error results
     * is that they contain an error code which helps understand what happened.
     *
     * @see KeeperException.Code
     */
    public static class ErrorResult extends OpResult {

        private int err;

        public ErrorResult(int err) {
            super(ZooDefs.OpCode.error);
            this.err = err;
        }

        public int getErr() {
            return err;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ErrorResult)) {
                return false;
            }

            ErrorResult other = (ErrorResult) o;
            return getType() == other.getType() && err == other.getErr();
        }

        @Override
        public int hashCode() {
            return getType() * 35 + err;
        }

    }

}
