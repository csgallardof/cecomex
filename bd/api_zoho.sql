-- phpMyAdmin SQL Dump
-- version 5.0.1
-- https://www.phpmyadmin.net/
--
-- Servidor: 127.0.0.1
-- Tiempo de generación: 05-03-2020 a las 15:57:54
-- Versión del servidor: 10.4.11-MariaDB
-- Versión de PHP: 7.3.14

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Base de datos: `api_zoho`
--

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `mba_cliente`
--

CREATE TABLE `mba_cliente` (
  `id` int(11) NOT NULL,
  `codigo_cliente` varchar(20) NOT NULL COMMENT 'codigo cliente',
  `nombre_cliente` varchar(150) NOT NULL COMMENT 'nombre cliente',
  `e_mail` varchar(50) NOT NULL COMMENT 'email cliente facturacion',
  `giro_negocio` int(11) NOT NULL,
  `vendedor_id` int(11) NOT NULL COMMENT 'id del vendedor',
  `limite_credito` decimal(10,2) NOT NULL COMMENT 'limite de pago',
  `terminos_de_pago` int(11) NOT NULL COMMENT 'terminos días de pago\r\nvalor predeterminado = 1',
  `codigo_regimen_fiscal` int(11) NOT NULL COMMENT 'CED - PASS - RUC ',
  `name_alt_razon_social` varchar(150) NOT NULL COMMENT 'nombre razon social',
  `usuar_razon_social` tinyint(1) NOT NULL COMMENT 'usuar razon social en documentos',
  `estado` varchar(50) NOT NULL COMMENT 'provincia facturacion',
  `ciudad_principal` int(50) NOT NULL COMMENT 'ciudad facturacion',
  `direccion_principal_1` text NOT NULL COMMENT 'domicilio facturacion',
  `email_fiscal` varchar(30) NOT NULL COMMENT 'mail de facturacion'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='l';

-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `tabla_prueba`
--

CREATE TABLE `tabla_prueba` (
  `id` int(11) NOT NULL,
  `nombre` varchar(50) NOT NULL,
  `apellido` varchar(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Volcado de datos para la tabla `tabla_prueba`
--

INSERT INTO `tabla_prueba` (`id`, `nombre`, `apellido`) VALUES
(1, 'Juan', 'Median'),
(2, 'Alberto', 'Rosales'),
(3, 'Juan', 'Median'),
(4, 'Alberto', 'Rosales');

--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `mba_cliente`
--
ALTER TABLE `mba_cliente`
  ADD PRIMARY KEY (`id`);

--
-- Indices de la tabla `tabla_prueba`
--
ALTER TABLE `tabla_prueba`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT de las tablas volcadas
--

--
-- AUTO_INCREMENT de la tabla `mba_cliente`
--
ALTER TABLE `mba_cliente`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT de la tabla `tabla_prueba`
--
ALTER TABLE `tabla_prueba`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
