#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdlib.h>
#include <mpi.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

int myid,numprocs,tag,root,destination,countre,count, proc, tam,source, result_timer;
MPI_Status status;
MPI_Request request;
int flag=0;
int array_cantidad_pendientes [4];

//Esta funcion es la que imprimira recursivamente el contenido
void arbol (char* path, bool procesar) {
	DIR* dirp;
	FILE *arch;
	int lSize;
	struct dirent* directorio;
	//se abre el directorio
	//si no existe la ruta introducida del directorio marcara error
	if ((dirp = opendir (path)) == NULL) {
		perror (path);
		return;
	}
	//ciclo donde se aplica la recursion
	while ((directorio = readdir (dirp)) != NULL) {
		//se manda a llamar la 'estructura' de la libreria stat.h
		struct stat buf;
		//Se especifica el tamaño total del fichero a procesar considerando la ruta absoluta junto con el nombre del archivo o directorio
		char fichero [strlen(path) + strlen(directorio->d_name) + 1];
		int i;
		//El valor true indica que se debe procesar los archivos que contiene fichero
		bool procesar_sig = true;
		//Revisa si hay mas niveles en el directorio
		if ((strcmp (directorio->d_name, ".") == 0) || (strcmp (directorio->d_name, "..") == 0))
		continue;
		//Se construye el path
		sprintf (fichero, "%s/%s", path, directorio->d_name);
		if (lstat (fichero, & buf) == -1) {
			perror(fichero);
			exit(1);
		}
		//si hay mas niveles se manda a llamar la funcion 'arbol' a si misma
		//para listar el contenido del sig subdirectorio
		if ((buf.st_mode & S_IFMT) == S_IFDIR) { 
			//Se consulta si hay mensajes pendientes
			MPI_Iprobe(source,MPI_ANY_TAG,MPI_COMM_WORLD,&flag,&status);
			if(flag){
				//Recibe el número del proceso que esta ocioso en proc
				MPI_Irecv(&proc,1,MPI_INT,source,tag,MPI_COMM_WORLD,&request);
				if(proc!=-1){
					MPI_Wait(&request,&status);
					tam=strlen(fichero);
					//Envìa el tamaño del path al proceso recibido en proc
					MPI_Isend(&tam,1,MPI_INT,proc,tag,MPI_COMM_WORLD,&request);
					//Envìa el path a proc para que procese los archivos que contiene
					MPI_Isend(&fichero,tam,MPI_CHAR,proc,tag,MPI_COMM_WORLD,&request);
					//Se indica que no debe procesar los archivos que contiene fichero
					procesar_sig = false;
					//Al estar ocioso el proceso, su lista de tareas pendientes esta vacía
					array_cantidad_pendientes[proc] = 0;
					proc = -1;
				}
			}
			else{
				//En caso de que no hayan procesos ociosos se asigna el directorio a procesar al proceso que tenga menos de 2 tareas pendientes
				for(int i = 0; i < numprocs; i ++){
					if((i!=0)&&(array_cantidad_pendientes[i] < 2)){
						tam=strlen(fichero);
						//Envìa el tamaño del path al proceso recibido en proc
						MPI_Isend(&tam,1,MPI_INT,i,tag,MPI_COMM_WORLD,&request);
						//Envìa el path a proc
						MPI_Isend(&fichero,tam,MPI_CHAR,i,tag,MPI_COMM_WORLD,&request);
						procesar_sig = false;
						break;
					}
				}
			}
			//En caso de que no se haya asignado el directorio a nadie, procesar_sig tiene valor true con lo que el root tendrá que procesar 
			//los archivos que contega fichero
			arbol (fichero, procesar_sig);
		}

		if(procesar){
			//Se calcula el tamaño del archivo si corresponde
			if (!(opendir(fichero))){
				arch=fopen(fichero,"r");
				fseek(arch,0,SEEK_END);
				lSize = ftell (arch);
				printf ("El fichero %s tiene tamaño %d\n", fichero,lSize);
			}
		}
	}
	//cierra el directorio
	closedir (dirp);
}

int procesarDirectorio(){
	//Espera tamaño del path
	MPI_Irecv(&tam,1,MPI_INT,0,tag,MPI_COMM_WORLD,&request);
	MPI_Wait(&request,&status);
	//Si recibe el valor cero significa que debe terminar su ejecución porque no hay más directorios para procesar
	if(tam != 0){
		char ruta [tam];
		//Espera el path
		MPI_Irecv(&ruta,tam,MPI_CHAR,0,tag,MPI_COMM_WORLD,&request);
		MPI_Wait(&request,&status);
		DIR* dirp;
		FILE *arch;
		int lSize;
		struct dirent* directorio;
		//se abre el directorio
		//si no existe la ruta introducida del directorio marcara error
		if ((dirp = opendir (ruta)) == NULL) {
			perror (ruta);
			return -1;
		}
		//ciclo donde se aplica la recursión
		while ((directorio = readdir (dirp)) != NULL) {
			//se manda a llamar la 'estructura' de la libreria stat.h
			struct stat buf;
			char fichero [strlen(ruta) + strlen(directorio->d_name) + 1];
			int i;
			//Revisa si hay mas niveles en el directorio
			if ((strcmp (directorio->d_name, ".") == 0) || (strcmp (directorio->d_name, "..") == 0))
			continue;
			sprintf (fichero, "%s/%s", ruta, directorio->d_name);
			if (!(opendir(fichero))){
				arch=fopen(fichero,"r");
				fseek(arch,0,SEEK_END);
				lSize = ftell (arch);
				printf ("El fichero %s tiene tamaño %d\n", fichero,lSize);
			}
		}
		//cierra el directorio
		closedir (dirp);
	}
	else{
		//Se indico que termine
		return tam;
	}
	return -1;
}
 
int main (int argc, char* argv []) {
	double tiempoinicial,tiempofinal;
	tiempoinicial=MPI_Wtime();
	MPI_Init(&argc,&argv);
	MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
	MPI_Comm_rank(MPI_COMM_WORLD,&myid);
	//Se inicializa el vector que controla las tareas pendientes
	for(int i = 0; i < numprocs; i ++){
		array_cantidad_pendientes[i]=0;
	}
	char *path="/home/eduardo/Escritorio/disco";
	root = 0; tag = 1234;
	source = MPI_ANY_SOURCE;
	if(myid==0){
		bool procesar = true;
		arbol(path, procesar);
		//Envía a todos los procesos que terminen
		for(int i=1;i<numprocs;i++){
			tam = 0;
			MPI_Isend(&tam,1,MPI_INT,i,tag,MPI_COMM_WORLD,&request);
		}
	}
	else{
		proc = myid;
		int res = -1;
		bool repetir = true;
		while(res != 0){
			//Envía al proceso root que está disponible para procesar
			repetir = true;
			//Le envia al root que está disponible para procesar
			MPI_Isend(&proc,1,MPI_INT,root,tag,MPI_COMM_WORLD,&request);
			res = procesarDirectorio();
			//Procesa las tareas pendientes
			while(repetir){
				MPI_Iprobe(0,MPI_ANY_TAG,MPI_COMM_WORLD,&flag,&status);
				if(flag){
					res=procesarDirectorio();
				}
				else{
					repetir = false;
				}
			}
		}
	}
	MPI_Finalize();
    tiempofinal=MPI_Wtime();
    printf("Tiempo: %f segundos\n",tiempofinal-tiempoinicial);
}