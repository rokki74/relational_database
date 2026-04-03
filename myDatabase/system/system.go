import(
	"log"
)

type DBSystem struct{
	Catalog CatalogManager
	BufferPool BufferPool
	Pager Pager
	SystemPath string
}

