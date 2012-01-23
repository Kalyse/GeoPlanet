<?php
use Doctrine\Common\ClassLoader;
set_time_limit(0);
define("BASE_PATH", __DIR__ . "/..");
# Load INI
$ini = BASE_PATH . "/application/configs/config.ini";
$parse = parse_ini_file($ini, true);
# Set Error Reporting
error_reporting(E_ALL);
ini_set('display_errors', 'On');

require BASE_PATH . '/library/Doctrine/Common/ClassLoader.php';
$classLoader = new ClassLoader('Doctrine', BASE_PATH . '/library');
$classLoader->register();

$config = new \Doctrine\DBAL\Configuration();

$connectionParams = array(
    'dbname' => $parse['mysql']['dbname'],
    'user' => $parse['mysql']['username'],
    'password' => $parse['mysql']['password'],
    'host' => $parse['mysql']['host'],
    'driver' => $parse['mysql']['driver'],
    'port' => $parse['mysql']['port'],
);
#Set up a Connection
$conn = \Doctrine\DBAL\DriverManager::getConnection($connectionParams, $config);

class NestedSetify {
    # Any root nodes in the table, will not have a parent id set, so it will be NULL. 

    private $_lft = "lft";
    private $_rgt = "rgt";
    private $_table = "geo_places";
    private $_parent = "parent_woeid";
    private $_id = "woeid";
    private $conn;

    public function __construct(Doctrine\DBAL\Connection $conn) {
        $this->conn = $conn;
    }

    public function transform() {
        $root = $this->getRoot();
        $this->_recursiveNestify($root[$this->_id], 1);
    }

    public function getConn() {
        return $this->conn;
    }

    private function getRoot() {
        # The table should only have ONE node that is the root, by having it's parent set as null. 
        $statement = $this->getConn()->prepare('SELECT * FROM `' . $this->_table . '` where ' . $this->_parent . ' is null limit 1');
        $statement->execute();
        $root = $statement->fetch();
        return $root;
    }

    function _recursiveNestify($parentId, $lft) {
        $rgt = $lft + 1;
        # Get all the children of this node, where the parentId = current node id. 
        $statement = $this->getConn()->prepare('SELECT '. $this->_id .' FROM `' . $this->_table . '` where ' . $this->_parent . ' = :parentId');
        $statement->bindValue("parentId", $parentId);
        $statement->execute();
        while ($node = $statement->fetch()){
            $rgt = $this->_recursiveNestify($node[$this->_id], $rgt );
        }
        // UPDATE tree SET lft='.$left.', rgt='.                  $right.' WHERE title="'.$parent.'";'
        $this->getConn()->executeUpdate('UPDATE `' . $this->_table . '` SET '. $this->_lft .' = ?,'. $this->_rgt .' = ?  WHERE ' . $this->_id . ' = ?', array($lft,$rgt,$parentId));
        return $rgt + 1;
    }
}
class MaterializedPathify {
    
    private $_path = "ancestry";
    private $_seperator = "/";
    private $_table = "geo_places";
    private $_parent = "parent_woeid";
    private $_id = "woeid";    
    private $conn;
    private $_nullPointToSelf = true;

    public function __construct(Doctrine\DBAL\Connection $conn) {
        $this->conn = $conn;
    }

    public function transform() {
        # FETCH EVERYTHING
        $statement = $this->getConn()->prepare('SELECT '.$this->_id .','. $this->_parent. ' FROM `' . $this->_table . '` order by '. $this->_id);
        $statement->execute();
        $model = array();
        while ($result = $statement->fetch()){
           $model[$result[$this->_id]] = $result[$this->_parent];
        }
      
        
        # model is not a in memory representation of the entire data set.
        # loop over it all, and keep digging deeper until parent is null. That means we have found the "root" and can construct the materialized path. 
        foreach($model as $key=>$node){
            $path = array();
            $parentPointer = $node;
            if(is_null($parentPointer) && $this->_nullPointToSelf){
              # If the node is already NULL, I want the ancestry to point to itself. (Even though it doesn't).
              $path[] = $key;  
            } else {
                while(!is_null($parentPointer)){
                    $path[] = $parentPointer;
                    $parentPointer = $model[$parentPointer];
                }
            }
            
            $ancestry = implode($this->_seperator,array_reverse( $path )) . $this->_seperator;
            # Now do all of the updates to set the ancestry. 
            $this->getConn()->executeUpdate('UPDATE `' . $this->_table . '` SET '. $this->_path .' = ? WHERE ' . $this->_id . ' = ?', array($ancestry, $key));
        }
        echo "Finished";
        
    }

    public function getConn() {
        return $this->conn;
    }
    
}

$ns = new NestedSetify($conn);
$mp = new MaterializedPathify($conn);
$ns->transform();
// $mp->transform();
