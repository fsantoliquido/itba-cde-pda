# itba-cde-pda
Repositorio para el trabajo práctico de Python Data Applications
## Configuración del proyecto

Para ejecutar este proyecto correctamente, necesitas configurar un archivo `.env` en la raíz del proyecto. Este archivo debe contener las siguientes variables:

```bash
YOUTUBE_API_KEY=TU_API_KEY

### Resumen de las ventajas:
- **Seguridad**: Las claves de API no quedan hardcodeadas en el código fuente.
- **Portabilidad**: Cada colaborador puede configurar sus propias credenciales de manera local, sin interferir con el código fuente del proyecto.
- **Compatibilidad con Docker**: Las variables de entorno pueden ser fácilmente incluidas en los contenedores de Docker mediante el archivo `.env`.

Con este enfoque, no sólo proteges las credenciales, sino que también garantizas que otros puedan ejecutar el proyecto fácilmente.