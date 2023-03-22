import logging as log


log.basicConfig(level=log.INFO,format= '%(asctime)s: %(levelname)s - %(message)s')
log.debug('Mensaje a nivel de debug ...')
log.info('Mensaje a  nivel de info ...')
log.warning('Mensaje a nivel de warning ...')
log.error('Mensaje a nivel de error ...')
log.critical('Mensaje de error critico')

