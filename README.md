# consistenthash
Fork of consistenthash submodule from [groupcache](https://github.com/golang/groupcache) project.

Support for removing key from consistent hash has been added.

The code has been rewritten with balanced BST. It has a better performance of update operations.

For use cases don't need to update keys dynamically, the [slice-based](https://github.com/zjx20/consistenthash/tree/slice-based) branch can be used, for lower memory footprint and better gets performance.
